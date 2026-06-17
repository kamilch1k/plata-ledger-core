package ledger

import "errors"

// Domain errors. Money is always represented in integer minor units
// (e.g. cents) — never floats — to avoid rounding bugs.
var (
	ErrInvalidAmount         = errors.New("amount must be a positive integer in minor units")
	ErrInvalidCurrency       = errors.New("currency must be a 3-letter ISO code")
	ErrCurrencyMismatch      = errors.New("from and to accounts have different currencies")
	ErrInsufficientFunds     = errors.New("insufficient funds")
	ErrAccountNotFound       = errors.New("account not found")
	ErrSameAccount           = errors.New("from and to accounts must differ")
	ErrMissingIdempotencyKey = errors.New("idempotency key is required")
)

// validCurrency reports whether c looks like an ISO-4217 currency code.
func validCurrency(c string) bool {
	if len(c) != 3 {
		return false
	}
	for _, r := range c {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}
