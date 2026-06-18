package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/kamilch1k/plata-ledger-core/internal/ledger"
	"github.com/kamilch1k/plata-ledger-core/internal/origination"
)

// LedgerService is the behaviour the HTTP layer needs. Decoupling from the
// concrete *ledger.Store keeps the handlers testable without a database.
type LedgerService interface {
	CreateAccount(ctx context.Context, name, currency string, initialMinor int64) (ledger.Account, error)
	GetAccount(ctx context.Context, id string) (ledger.Account, error)
	Transfer(ctx context.Context, p ledger.TransferParams) (ledger.Transfer, bool, error)
	Statement(ctx context.Context, accountID string, limit int) ([]ledger.Entry, error)
}

type server struct {
	svc  LedgerService
	orig OriginationService
}

// NewServer wires the routes. Go 1.22+ pattern routing keeps it dependency-free.
// orig may be nil, in which case the origination endpoints are not registered.
func NewServer(svc LedgerService, orig OriginationService) http.Handler {
	s := &server{svc: svc, orig: orig}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /accounts", s.createAccount)
	mux.HandleFunc("GET /accounts/{id}", s.getAccount)
	mux.HandleFunc("GET /accounts/{id}/statement", s.statement)
	mux.HandleFunc("POST /transfers", s.createTransfer)
	if orig != nil {
		s.registerOrigination(mux)
	}
	return mux
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type createAccountRequest struct {
	Name         string `json:"name"`
	Currency     string `json:"currency"`
	InitialMinor int64  `json:"initial_balance_minor"`
}

func (s *server) createAccount(w http.ResponseWriter, r *http.Request) {
	var req createAccountRequest
	if !decode(w, r, &req) {
		return
	}
	acc, err := s.svc.CreateAccount(r.Context(), req.Name, req.Currency, req.InitialMinor)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, acc)
}

func (s *server) getAccount(w http.ResponseWriter, r *http.Request) {
	acc, err := s.svc.GetAccount(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, acc)
}

func (s *server) statement(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	entries, err := s.svc.Statement(r.Context(), r.PathValue("id"), limit)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

type transferRequest struct {
	From        string `json:"from_account"`
	To          string `json:"to_account"`
	AmountMinor int64  `json:"amount_minor"`
}

func (s *server) createTransfer(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeErr(w, ledger.ErrMissingIdempotencyKey)
		return
	}
	var req transferRequest
	if !decode(w, r, &req) {
		return
	}
	t, replay, err := s.svc.Transfer(r.Context(), ledger.TransferParams{
		IdempotencyKey: key,
		FromAccount:    req.From,
		ToAccount:      req.To,
		AmountMinor:    req.AmountMinor,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	// A replay is acknowledged with 200; a freshly-applied transfer with 201.
	status := http.StatusCreated
	if replay {
		status = http.StatusOK
	}
	writeJSON(w, status, t)
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// writeErr maps domain errors to HTTP status codes.
func writeErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ledger.ErrAccountNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ledger.ErrInsufficientFunds),
		errors.Is(err, ledger.ErrCurrencyMismatch):
		status = http.StatusUnprocessableEntity
	case errors.Is(err, ledger.ErrInvalidAmount),
		errors.Is(err, ledger.ErrInvalidCurrency),
		errors.Is(err, ledger.ErrSameAccount),
		errors.Is(err, ledger.ErrMissingIdempotencyKey),
		errors.Is(err, origination.ErrInvalidApplication):
		status = http.StatusBadRequest
	case errors.Is(err, origination.ErrApplicationNotFound):
		status = http.StatusNotFound
	case errors.Is(err, origination.ErrIllegalTransition):
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
