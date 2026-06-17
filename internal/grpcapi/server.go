package grpcapi

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kamilch1k/plata-ledger-core/internal/ledger"
	pb "github.com/kamilch1k/plata-ledger-core/internal/ledgerpb"
)

// LedgerService is the behaviour the gRPC layer needs; *ledger.Store satisfies it.
type LedgerService interface {
	CreateAccount(ctx context.Context, name, currency string, initialMinor int64) (ledger.Account, error)
	GetAccount(ctx context.Context, id string) (ledger.Account, error)
	Transfer(ctx context.Context, p ledger.TransferParams) (ledger.Transfer, bool, error)
}

// Server adapts LedgerService to the generated gRPC service.
type Server struct {
	pb.UnimplementedLedgerServer
	svc LedgerService
}

func NewServer(svc LedgerService) *Server { return &Server{svc: svc} }

func (s *Server) CreateAccount(ctx context.Context, r *pb.CreateAccountRequest) (*pb.Account, error) {
	a, err := s.svc.CreateAccount(ctx, r.GetName(), r.GetCurrency(), r.GetInitialBalanceMinor())
	if err != nil {
		return nil, toStatus(err)
	}
	return toPBAccount(a), nil
}

func (s *Server) GetAccount(ctx context.Context, r *pb.GetAccountRequest) (*pb.Account, error) {
	a, err := s.svc.GetAccount(ctx, r.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return toPBAccount(a), nil
}

func (s *Server) Transfer(ctx context.Context, r *pb.TransferRequest) (*pb.TransferResponse, error) {
	t, replay, err := s.svc.Transfer(ctx, ledger.TransferParams{
		IdempotencyKey: r.GetIdempotencyKey(),
		FromAccount:    r.GetFromAccount(),
		ToAccount:      r.GetToAccount(),
		AmountMinor:    r.GetAmountMinor(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.TransferResponse{
		Id:          t.ID,
		FromAccount: t.FromAccount,
		ToAccount:   t.ToAccount,
		AmountMinor: t.AmountMinor,
		Currency:    t.Currency,
		Replay:      replay,
	}, nil
}

func toPBAccount(a ledger.Account) *pb.Account {
	return &pb.Account{Id: a.ID, Name: a.Name, Currency: a.Currency, BalanceMinor: a.BalanceMinor}
}

// toStatus maps domain errors to gRPC status codes.
func toStatus(err error) error {
	switch {
	case errors.Is(err, ledger.ErrAccountNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ledger.ErrInsufficientFunds), errors.Is(err, ledger.ErrCurrencyMismatch):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ledger.ErrInvalidAmount),
		errors.Is(err, ledger.ErrInvalidCurrency),
		errors.Is(err, ledger.ErrSameAccount),
		errors.Is(err, ledger.ErrMissingIdempotencyKey):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
