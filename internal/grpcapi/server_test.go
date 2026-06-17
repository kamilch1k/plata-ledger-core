package grpcapi

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/kamilch1k/plata-ledger-core/internal/ledger"
	pb "github.com/kamilch1k/plata-ledger-core/internal/ledgerpb"
)

// fakeService implements LedgerService in memory so the gRPC layer is tested
// without a database.
type fakeService struct {
	mu   sync.Mutex
	seen map[string]ledger.Transfer
}

func newFake() *fakeService { return &fakeService{seen: map[string]ledger.Transfer{}} }

func (f *fakeService) CreateAccount(_ context.Context, name, currency string, initial int64) (ledger.Account, error) {
	if currency == "bad" {
		return ledger.Account{}, ledger.ErrInvalidCurrency
	}
	return ledger.Account{ID: "acc_1", Name: name, Currency: currency, BalanceMinor: initial}, nil
}

func (f *fakeService) GetAccount(_ context.Context, id string) (ledger.Account, error) {
	if id == "acc_missing" {
		return ledger.Account{}, ledger.ErrAccountNotFound
	}
	return ledger.Account{ID: id, Currency: "USD", BalanceMinor: 1_000}, nil
}

func (f *fakeService) Transfer(_ context.Context, p ledger.TransferParams) (ledger.Transfer, bool, error) {
	if p.AmountMinor > 1_000 {
		return ledger.Transfer{}, false, ledger.ErrInsufficientFunds
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.seen[p.IdempotencyKey]; ok {
		return t, true, nil
	}
	t := ledger.Transfer{ID: "txn_" + p.IdempotencyKey, FromAccount: p.FromAccount, ToAccount: p.ToAccount, AmountMinor: p.AmountMinor, Currency: "USD"}
	f.seen[p.IdempotencyKey] = t
	return t, false, nil
}

func dial(t *testing.T, svc LedgerService) pb.LedgerClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pb.RegisterLedgerServer(srv, NewServer(svc))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewLedgerClient(conn)
}

func TestGRPC_CreateAndTransfer_Idempotent(t *testing.T) {
	client := dial(t, newFake())
	ctx := context.Background()

	if _, err := client.CreateAccount(ctx, &pb.CreateAccountRequest{Name: "A", Currency: "USD", InitialBalanceMinor: 1_000}); err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}

	first, err := client.Transfer(ctx, &pb.TransferRequest{IdempotencyKey: "k", FromAccount: "acc_1", ToAccount: "acc_2", AmountMinor: 100})
	if err != nil {
		t.Fatalf("Transfer: %v", err)
	}
	if first.GetReplay() {
		t.Fatal("first delivery should not be a replay")
	}
	second, err := client.Transfer(ctx, &pb.TransferRequest{IdempotencyKey: "k", FromAccount: "acc_1", ToAccount: "acc_2", AmountMinor: 100})
	if err != nil {
		t.Fatalf("Transfer replay: %v", err)
	}
	if !second.GetReplay() {
		t.Fatal("second delivery with same key should be a replay")
	}
	if first.GetId() != second.GetId() {
		t.Fatal("replay should return the same transfer id")
	}
}

func TestGRPC_InsufficientFunds_MapsToFailedPrecondition(t *testing.T) {
	client := dial(t, newFake())
	_, err := client.Transfer(context.Background(), &pb.TransferRequest{IdempotencyKey: "big", FromAccount: "acc_1", ToAccount: "acc_2", AmountMinor: 5_000})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got code %v, want FailedPrecondition", status.Code(err))
	}
}

func TestGRPC_GetAccount_NotFound(t *testing.T) {
	client := dial(t, newFake())
	_, err := client.GetAccount(context.Background(), &pb.GetAccountRequest{Id: "acc_missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("got code %v, want NotFound", status.Code(err))
	}
}
