// Package origination implements an event-sourced loan-application state machine:
//
//	submitted -> kyc_passed | kyc_failed
//	kyc_passed -> scored
//	scored     -> approved | declined
//	approved   -> disbursed
//
// Every transition appends an event in the same transaction as the projection
// update (transactional outbox), so the event log is the source of truth.
package origination

import (
	"errors"
	"time"
)

type State string

const (
	Submitted State = "submitted"
	KYCPassed State = "kyc_passed"
	KYCFailed State = "kyc_failed"
	Scored    State = "scored"
	Approved  State = "approved"
	Declined  State = "declined"
	Disbursed State = "disbursed"
)

// transitions is the single source of truth for which moves are legal.
var transitions = map[State]map[State]bool{
	Submitted: {KYCPassed: true, KYCFailed: true},
	KYCPassed: {Scored: true},
	Scored:    {Approved: true, Declined: true},
	Approved:  {Disbursed: true},
	KYCFailed: {},
	Declined:  {},
	Disbursed: {},
}

// CanTransition reports whether from -> to is a legal move.
func CanTransition(from, to State) bool {
	return transitions[from][to]
}

// ApprovalThreshold is the minimum risk score required to approve.
const ApprovalThreshold = 60

type Application struct {
	ID                  string    `json:"id"`
	BorrowerAccount     string    `json:"borrower_account"`
	AmountMinor         int64     `json:"amount_minor"`
	TermMonths          int       `json:"term_months"`
	MonthlyIncomeMinor  int64     `json:"monthly_income_minor"`
	State               State     `json:"state"`
	RiskScore           int       `json:"risk_score"`
	DisbursedTransferID string    `json:"disbursed_transfer_id,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// RiskScore is a transparent, rule-based score in [0,100]. It is intentionally
// simple and explainable: the dominant factor is the monthly-payment-to-income
// ratio, with a small bonus for smaller principals.
func RiskScore(a Application) int {
	score := 50

	term := a.TermMonths
	if term < 1 {
		term = 1
	}
	monthlyPayment := a.AmountMinor / int64(term)

	if a.MonthlyIncomeMinor > 0 {
		ratio := float64(monthlyPayment) / float64(a.MonthlyIncomeMinor)
		switch {
		case ratio < 0.10:
			score += 40
		case ratio < 0.20:
			score += 25
		case ratio < 0.35:
			score += 10
		default:
			score -= 20
		}
	} else {
		score -= 20
	}

	if a.AmountMinor <= 1_000_000 { // small loans are lower risk
		score += 10
	}

	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	return score
}

var (
	ErrApplicationNotFound = errors.New("application not found")
	ErrIllegalTransition   = errors.New("illegal state transition")
	ErrInvalidApplication  = errors.New("invalid application input")
)
