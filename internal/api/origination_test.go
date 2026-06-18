package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/kamilch1k/ledgerline/internal/origination"
)

// fakeOrig implements OriginationService in memory.
type fakeOrig struct{}

func (fakeOrig) CreateApplication(_ context.Context, in origination.CreateApplicationInput) (origination.Application, error) {
	if in.AmountMinor <= 0 {
		return origination.Application{}, origination.ErrInvalidApplication
	}
	return origination.Application{ID: "app_1", BorrowerAccount: in.BorrowerAccount, AmountMinor: in.AmountMinor, State: origination.Submitted}, nil
}

func (fakeOrig) Get(_ context.Context, id string) (origination.Application, error) {
	if id == "app_missing" {
		return origination.Application{}, origination.ErrApplicationNotFound
	}
	return origination.Application{ID: id, State: origination.Submitted}, nil
}

func (fakeOrig) RunKYC(_ context.Context, id string, _ bool) (origination.Application, error) {
	if id == "app_wrongstate" {
		return origination.Application{}, origination.ErrIllegalTransition
	}
	return origination.Application{ID: id, State: origination.KYCPassed}, nil
}

func (fakeOrig) Score(_ context.Context, id string) (origination.Application, error) {
	return origination.Application{ID: id, State: origination.Scored, RiskScore: 80}, nil
}

func (fakeOrig) Decide(_ context.Context, id string) (origination.Application, error) {
	return origination.Application{ID: id, State: origination.Approved}, nil
}

func (fakeOrig) Disburse(_ context.Context, id, _ string) (origination.Application, error) {
	return origination.Application{ID: id, State: origination.Disbursed}, nil
}

func origServer() http.Handler { return NewServer(newFake(), fakeOrig{}) }

func TestCreateApplication_201(t *testing.T) {
	rr := do(t, origServer(), "POST", "/applications", nil,
		map[string]any{"borrower_account": "acc_1", "amount_minor": 50000, "term_months": 12, "monthly_income_minor": 400000})
	if rr.Code != http.StatusCreated {
		t.Fatalf("got %d, want 201", rr.Code)
	}
}

func TestApplication_TransitionsReturn200(t *testing.T) {
	h := origServer()
	for _, path := range []string{
		"/applications/app_1/score",
		"/applications/app_1/decision",
	} {
		rr := do(t, h, "POST", path, nil, nil)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s: got %d, want 200", path, rr.Code)
		}
	}
	// kyc and disburse take a body
	if rr := do(t, h, "POST", "/applications/app_1/kyc", nil, map[string]any{"pass": true}); rr.Code != http.StatusOK {
		t.Fatalf("kyc: got %d, want 200", rr.Code)
	}
	if rr := do(t, h, "POST", "/applications/app_1/disburse", nil, map[string]any{"funding_account": "acc_fund"}); rr.Code != http.StatusOK {
		t.Fatalf("disburse: got %d, want 200", rr.Code)
	}
}

func TestApplication_NotFound_404(t *testing.T) {
	rr := do(t, origServer(), "GET", "/applications/app_missing", nil, nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404", rr.Code)
	}
}

func TestApplication_IllegalTransition_409(t *testing.T) {
	rr := do(t, origServer(), "POST", "/applications/app_wrongstate/kyc", nil, map[string]any{"pass": true})
	if rr.Code != http.StatusConflict {
		t.Fatalf("got %d, want 409", rr.Code)
	}
}

func TestApplication_RoutesAbsentWhenNil(t *testing.T) {
	// With a nil OriginationService the routes are not registered.
	rr := do(t, NewServer(newFake(), nil), "POST", "/applications", nil, map[string]any{"amount_minor": 1})
	if rr.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404 (route not registered)", rr.Code)
	}
}
