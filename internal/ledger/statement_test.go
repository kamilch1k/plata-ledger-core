package ledger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatement_And_Conservation(t *testing.T) {
	ctx := context.Background()
	s := freshStore(t)
	a, _ := s.CreateAccount(ctx, "A", "USD", 1_000)
	b, _ := s.CreateAccount(ctx, "B", "USD", 500)

	total0, err := s.SumBalances(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1_500, total0)

	_, _, err = s.Transfer(ctx, TransferParams{
		IdempotencyKey: "s1", FromAccount: a.ID, ToAccount: b.ID, AmountMinor: 200,
	})
	require.NoError(t, err)

	entriesA, err := s.Statement(ctx, a.ID, 100)
	require.NoError(t, err)
	require.Len(t, entriesA, 1)
	require.EqualValues(t, -200, entriesA[0].AmountMinor)

	entriesB, err := s.Statement(ctx, b.ID, 0) // 0 -> default limit
	require.NoError(t, err)
	require.Len(t, entriesB, 1)
	require.EqualValues(t, 200, entriesB[0].AmountMinor)

	total1, err := s.SumBalances(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1_500, total1, "transfers conserve the total balance")
}
