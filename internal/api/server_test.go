package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/kamilch1k/plata-ledger-core/internal/ledger"
)

// fakeService implements LedgerService in memory so the HTTP layer is tested
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

func (f *fakeService) Statement(_ context.Context, _ string, _ int) ([]ledger.Entry, error) {
	return []ledger.Entry{}, nil
}

func do(t *testing.T, h http.Handler, method, path string, header map[string]string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestCreateAccount_201(t *testing.T) {
	h := NewServer(newFake(), nil)
	rr := do(t, h, "POST", "/accounts", nil, map[string]any{"name": "Alice", "currency": "USD", "initial_balance_minor": 500})
	if rr.Code != http.StatusCreated {
		t.Fatalf("got %d, want 201", rr.Code)
	}
}

func TestTransfer_RequiresIdempotencyKey(t *testing.T) {
	h := NewServer(newFake(), nil)
	rr := do(t, h, "POST", "/transfers", nil, map[string]any{"from_account": "acc_1", "to_account": "acc_2", "amount_minor": 100})
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", rr.Code)
	}
}

func TestTransfer_NewThenReplay(t *testing.T) {
	h := NewServer(newFake(), nil)
	hdr := map[string]string{"Idempotency-Key": "abc"}
	body := map[string]any{"from_account": "acc_1", "to_account": "acc_2", "amount_minor": 100}

	first := do(t, h, "POST", "/transfers", hdr, body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first: got %d, want 201", first.Code)
	}
	second := do(t, h, "POST", "/transfers", hdr, body)
	if second.Code != http.StatusOK {
		t.Fatalf("replay: got %d, want 200", second.Code)
	}
}

func TestTransfer_InsufficientFunds_422(t *testing.T) {
	h := NewServer(newFake(), nil)
	rr := do(t, h, "POST", "/transfers", map[string]string{"Idempotency-Key": "big"},
		map[string]any{"from_account": "acc_1", "to_account": "acc_2", "amount_minor": 5_000})
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("got %d, want 422", rr.Code)
	}
}

func TestGetAccount_404(t *testing.T) {
	h := NewServer(newFake(), nil)
	rr := do(t, h, "GET", "/accounts/acc_missing", nil, nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404", rr.Code)
	}
}
