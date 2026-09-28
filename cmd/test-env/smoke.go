package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"
)

type dialFunc func(ctx context.Context) (net.Conn, error)

// smoke binds as the smoke SMPP account, submits a message and waits for its DLR. ctx carries
// the overall deadline; retry is the interval between two refused binds (rotation takes a moment to
// reach session-manager); dlrWait bounds the wait for one submission's DLR before resubmitting.
func smoke(ctx context.Context, a *admin, dial dialFunc, retry, dlrWait time.Duration) error {
	accountID, err := findSmokeAccount(ctx, a)
	if err != nil {
		return err
	}
	secret, err := rotateSmokeSecret(ctx, a, accountID)
	if err != nil {
		return err
	}

	nc, err := bindWithRetry(ctx, dial, secret, retry)
	if err != nil {
		return err
	}
	defer func() { _ = nc.Close() }()

	return submitUntilDLR(ctx, nc, dlrWait)
}

// rotateSmokeSecret rotates the smoke credential so the bind below never uses a secret the operator
// (or another test run) already saw.
func rotateSmokeSecret(ctx context.Context, a *admin, accountID string) (string, error) {
	credID, err := activeCredentialID(ctx, a, accountID)
	if err != nil {
		return "", err
	}
	if credID == "" {
		return "", fmt.Errorf("credential %q : introuvable ou inactive", smokeSystemID)
	}

	var rotated struct {
		Secret string `json:"secret"`
	}
	if err := a.do(ctx, http.MethodPost, "/smpp-accounts/"+accountID+"/credentials/"+credID+"/rotate", nil, &rotated); err != nil {
		return "", err
	}
	return rotated.Secret, nil
}

func bindWithRetry(ctx context.Context, dial dialFunc, secret string, retry time.Duration) (net.Conn, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		nc, err := dial(ctx)
		if err != nil {
			return nil, fmt.Errorf("connexion SMPP : %w", err)
		}
		if deadline, ok := ctx.Deadline(); ok {
			if err := nc.SetDeadline(deadline); err != nil {
				_ = nc.Close()
				return nil, fmt.Errorf("échéance de connexion : %w", err)
			}
		}

		req := smpp.PDU{Sequence: 1, Body: &smpp.BindTransceiver{BindFields: smpp.BindFields{
			SystemID:         smokeSystemID,
			Password:         secret,
			InterfaceVersion: smpp.InterfaceVersion34,
		}}}
		if err := smpp.WritePDU(nc, req); err != nil {
			_ = nc.Close()
			return nil, fmt.Errorf("écriture bind_transceiver : %w", err)
		}
		resp, err := smpp.ReadPDU(nc)
		if err != nil {
			_ = nc.Close()
			return nil, fmt.Errorf("lecture bind_transceiver_resp : %w", err)
		}
		if resp.Status == smpp.StatusOK {
			return nc, nil
		}
		_ = nc.Close()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retry):
		}
	}
}

// submitUntilDLR resubmits after each dlrWait without a DLR, until ctx is done: connector-pool-svc,
// restarted onto a new CONNECTOR_ID, consumes mt.routed through a fresh group that starts at the
// topic's end and can miss the first message. Any submitted message's DLR ends the test.
func submitUntilDLR(ctx context.Context, nc net.Conn, dlrWait time.Duration) error {
	deadline, hasDeadline := ctx.Deadline()
	submitted := map[string]bool{}
	seq := uint32(1)

	for {
		seq++
		wait := time.Now().Add(dlrWait)
		lastRound := hasDeadline && !deadline.After(wait)
		if lastRound {
			wait = deadline
		}
		if err := nc.SetDeadline(wait); err != nil {
			return fmt.Errorf("échéance de connexion : %w", err)
		}
		if err := smpp.WritePDU(nc, smokeSubmit(seq)); err != nil {
			return fmt.Errorf("écriture submit_sm : %w", err)
		}

		received, err := readUntilDLR(nc, submitted)
		var netErr net.Error
		switch {
		case received:
			seq++
			_ = smpp.WritePDU(nc, smpp.PDU{Sequence: seq, Body: &smpp.Unbind{}})
			return nil
		case errors.As(err, &netErr) && netErr.Timeout() && !lastRound:
			continue
		default:
			return fmt.Errorf("aucun DLR pour %s : %w", strings.Join(slices.Sorted(maps.Keys(submitted)), ", "), err)
		}
	}
}

func smokeSubmit(seq uint32) smpp.PDU {
	return smpp.PDU{Sequence: seq, Body: &smpp.SubmitSM{SMFields: smpp.SMFields{
		SourceAddrTON:      5,
		SourceAddr:         senderAddr,
		DestAddrTON:        1,
		DestAddrNPI:        1,
		DestinationAddr:    "33612345678",
		RegisteredDelivery: smpp.RegisteredDeliveryReceipt,
		ShortMessage:       []byte("step-275 smoke"),
	}}}
}

func readUntilDLR(nc net.Conn, submitted map[string]bool) (bool, error) {
	for {
		pdu, err := smpp.ReadPDU(nc)
		if err != nil {
			return false, err
		}

		switch body := pdu.Body.(type) {
		case *smpp.SubmitSMResp:
			if pdu.Status != smpp.StatusOK {
				return false, fmt.Errorf("submit_sm refusé, command_status %#08x", pdu.Status)
			}
			submitted[body.MessageID] = true
		case *smpp.EnquireLink:
			if err := smpp.WritePDU(nc, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.EnquireLinkResp{}}); err != nil {
				return false, fmt.Errorf("écriture enquire_link_resp : %w", err)
			}
		case *smpp.DeliverSM:
			if err := smpp.WritePDU(nc, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.DeliverSMResp{}}); err != nil {
				return false, fmt.Errorf("écriture deliver_sm_resp : %w", err)
			}
			if body.ESMClass&smpp.ESMClassMCDeliveryReceipt == 0 {
				continue
			}
			receipt, ok := strings.CutPrefix(string(body.ShortMessage), "id:")
			msgID, _, _ := strings.Cut(receipt, " ")
			if ok && submitted[msgID] {
				return true, nil
			}
		}
	}
}
