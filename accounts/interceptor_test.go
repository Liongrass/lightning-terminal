package accounts

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/clock"
	"github.com/lightningnetwork/lnd/fn"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/macaroons"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"gopkg.in/macaroon-bakery.v2/bakery/checkers"
	"gopkg.in/macaroon.v2"
)

// TestAccountIDCaveatEmbedding tests that the account ID can be embedded in a
// macaroon caveat and extracted from it.
func TestAccountIDCaveatEmbedding(t *testing.T) {
	badCondition := checkers.Condition(macaroons.CondLndCustom, fmt.Sprintf(
		"%s %s", CondAccount, "invalid hex",
	))

	tests := []struct {
		name         string
		caveats      []macaroon.Caveat
		expectedErr  string
		expectedAcct fn.Option[AccountID]
	}{
		{
			name: "valid account ID, single caveat",
			caveats: []macaroon.Caveat{
				CaveatFromID(AccountID{1, 2, 3, 4, 5}),
			},
			expectedAcct: fn.Some(AccountID{1, 2, 3, 4, 5}),
		},
		{
			name: "valid account ID, single multiple caveats",
			caveats: []macaroon.Caveat{
				{Id: []byte("some other caveat")},
				CaveatFromID(AccountID{1, 2, 3, 4, 5}),
				{Id: []byte("another one")},
			},
			expectedAcct: fn.Some(AccountID{1, 2, 3, 4, 5}),
		},
		{
			name: "invalid account ID",
			caveats: []macaroon.Caveat{
				{Id: []byte(badCondition)},
			},
			expectedErr: "encoding/hex: invalid",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			acct, err := IDFromCaveats(test.caveats)
			if test.expectedErr != "" {
				require.ErrorContains(t, err, test.expectedErr)

				return
			}
			require.NoError(t, err)

			if test.expectedAcct.IsNone() {
				require.True(t, acct.IsNone())

				return
			}
			require.True(t, acct.IsSome())

			test.expectedAcct.WhenSome(func(id AccountID) {
				acct.WhenSome(func(acct AccountID) {
					require.Equal(t, id, acct)
				})
			})
		})
	}
}

// TestInterceptSubscribeInvoices makes sure that invoice updates sent on a
// SubscribeInvoices stream are only delivered to the client if the invoice
// belongs to the account of the macaroon, and are dropped otherwise.
func TestInterceptSubscribeInvoices(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestDB(t, clock.NewTestClock(time.Now()))
	service, err := NewService(store, func(error) {})
	require.NoError(t, err)
	require.NoError(t, service.Start(
		ctx, newMockLnd(), newMockRouter(), chainParams,
		WithStreamMessageDrop(),
	))
	t.Cleanup(func() {
		_ = service.Stop()
	})

	acct, err := service.NewAccount(ctx, 1000, testExpiration, "account")
	require.NoError(t, err)

	ownHash := lntypes.Hash{1, 2, 3}
	otherHash := lntypes.Hash{4, 5, 6}
	require.NoError(t, service.AssociateInvoice(ctx, acct.ID, ownHash))

	mac, err := macaroon.New(
		[]byte("root-key"), []byte("id"), "lnd", macaroon.LatestVersion,
	)
	require.NoError(t, err)
	require.NoError(t, mac.AddFirstPartyCaveat(CaveatFromID(acct.ID).Id))
	rawMac, err := mac.MarshalBinary()
	require.NoError(t, err)

	intercept := func(hash lntypes.Hash) *lnrpc.InterceptFeedback {
		serialized, err := proto.Marshal(&lnrpc.Invoice{
			RHash: hash[:],
		})
		require.NoError(t, err)

		resp, err := service.Intercept(ctx, &lnrpc.RPCMiddlewareRequest{
			RequestId:   1,
			MsgId:       2,
			RawMacaroon: rawMac,
			InterceptType: &lnrpc.RPCMiddlewareRequest_Response{
				Response: &lnrpc.RPCMessage{
					MethodFullUri: "/lnrpc.Lightning/" +
						"SubscribeInvoices",
					StreamRpc:  true,
					TypeName:   "lnrpc.Invoice",
					Serialized: serialized,
				},
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, resp.RefMsgId)

		return resp.GetFeedback()
	}

	// An update for an invoice of the account is passed through as is.
	feedback := intercept(ownHash)
	require.Empty(t, feedback.Error)
	require.False(t, feedback.DropMessage)
	require.False(t, feedback.ReplaceResponse)

	// An update for an invoice that doesn't belong to the account is
	// dropped, without terminating the stream with an error.
	feedback = intercept(otherHash)
	require.Empty(t, feedback.Error)
	require.True(t, feedback.DropMessage)
	require.False(t, feedback.ReplaceResponse)
}
