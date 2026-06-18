package api

import (
	"context"
	"net/http"

	"github.com/kamilch1k/plata-ledger-core/internal/origination"
)

// OriginationService is the loan-origination behaviour the HTTP layer needs;
// *origination.Store satisfies it.
type OriginationService interface {
	CreateApplication(ctx context.Context, in origination.CreateApplicationInput) (origination.Application, error)
	Get(ctx context.Context, id string) (origination.Application, error)
	RunKYC(ctx context.Context, id string, pass bool) (origination.Application, error)
	Score(ctx context.Context, id string) (origination.Application, error)
	Decide(ctx context.Context, id string) (origination.Application, error)
	Disburse(ctx context.Context, id, fundingAccount string) (origination.Application, error)
}

func (s *server) registerOrigination(mux *http.ServeMux) {
	mux.HandleFunc("POST /applications", s.createApplication)
	mux.HandleFunc("GET /applications/{id}", s.getApplication)
	mux.HandleFunc("POST /applications/{id}/kyc", s.runKYC)
	mux.HandleFunc("POST /applications/{id}/score", s.scoreApplication)
	mux.HandleFunc("POST /applications/{id}/decision", s.decideApplication)
	mux.HandleFunc("POST /applications/{id}/disburse", s.disburseApplication)
}

type createApplicationRequest struct {
	BorrowerAccount    string `json:"borrower_account"`
	AmountMinor        int64  `json:"amount_minor"`
	TermMonths         int    `json:"term_months"`
	MonthlyIncomeMinor int64  `json:"monthly_income_minor"`
}

func (s *server) createApplication(w http.ResponseWriter, r *http.Request) {
	var req createApplicationRequest
	if !decode(w, r, &req) {
		return
	}
	app, err := s.orig.CreateApplication(r.Context(), origination.CreateApplicationInput{
		BorrowerAccount:    req.BorrowerAccount,
		AmountMinor:        req.AmountMinor,
		TermMonths:         req.TermMonths,
		MonthlyIncomeMinor: req.MonthlyIncomeMinor,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, app)
}

func (s *server) getApplication(w http.ResponseWriter, r *http.Request) {
	app, err := s.orig.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, app)
}

type kycRequest struct {
	Pass bool `json:"pass"`
}

func (s *server) runKYC(w http.ResponseWriter, r *http.Request) {
	var req kycRequest
	if !decode(w, r, &req) {
		return
	}
	s.applyTransition(w, r, func(ctx context.Context, id string) (origination.Application, error) {
		return s.orig.RunKYC(ctx, id, req.Pass)
	})
}

func (s *server) scoreApplication(w http.ResponseWriter, r *http.Request) {
	s.applyTransition(w, r, s.orig.Score)
}

func (s *server) decideApplication(w http.ResponseWriter, r *http.Request) {
	s.applyTransition(w, r, s.orig.Decide)
}

type disburseRequest struct {
	FundingAccount string `json:"funding_account"`
}

func (s *server) disburseApplication(w http.ResponseWriter, r *http.Request) {
	var req disburseRequest
	if !decode(w, r, &req) {
		return
	}
	s.applyTransition(w, r, func(ctx context.Context, id string) (origination.Application, error) {
		return s.orig.Disburse(ctx, id, req.FundingAccount)
	})
}

// applyTransition runs a state transition for the application in the path and
// writes the resulting application or the mapped error.
func (s *server) applyTransition(w http.ResponseWriter, r *http.Request, fn func(context.Context, string) (origination.Application, error)) {
	app, err := fn(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, app)
}
