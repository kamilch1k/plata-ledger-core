package ledger

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// Property: for any random sequence of transfers among a fixed set of accounts,
// the ledger stays balanced (entries sum to zero), the total balance is
// conserved, and no balance ever goes negative.
func TestProperty_LedgerInvariants(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 40
	properties := gopter.NewProperties(params)

	properties.Property("random transfers keep books balanced, conserved, and non-negative",
		prop.ForAll(func(seed int64) bool {
			s := freshStore(t)
			ctx := context.Background()

			const accounts = 4
			const each = 1_000
			ids := make([]string, 0, accounts)
			for i := 0; i < accounts; i++ {
				a, err := s.CreateAccount(ctx, "acct", "USD", each)
				if err != nil {
					return false
				}
				ids = append(ids, a.ID)
			}

			rng := rand.New(rand.NewSource(seed))
			ops := 20 + rng.Intn(40)
			for i := 0; i < ops; i++ {
				_, _, err := s.Transfer(ctx, TransferParams{
					IdempotencyKey: fmt.Sprintf("p%d-%d", seed, i),
					FromAccount:    ids[rng.Intn(accounts)],
					ToAccount:      ids[rng.Intn(accounts)],
					AmountMinor:    int64(rng.Intn(400) + 1),
				})
				// The only acceptable failures are domain rejections.
				if err != nil && !errors.Is(err, ErrInsufficientFunds) && !errors.Is(err, ErrSameAccount) {
					return false
				}
			}

			sumEntries, err := s.SumLedgerEntries(ctx)
			if err != nil || sumEntries != 0 {
				return false
			}
			sumBalances, err := s.SumBalances(ctx)
			if err != nil || sumBalances != int64(accounts*each) {
				return false
			}
			for _, id := range ids {
				a, err := s.GetAccount(ctx, id)
				if err != nil || a.BalanceMinor < 0 {
					return false
				}
			}
			return true
		}, gen.Int64()))

	properties.TestingRun(t)
}
