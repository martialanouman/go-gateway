package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"
)

// dialFunc opens the connection smoke binds over. Tests inject a plain TCP dial to an in-process
// peer; production dials TLS to smpp-server-svc:2775.
type dialFunc func(ctx context.Context) (net.Conn, error)

type credentialSecret struct {
	Secret string `json:"secret"`
}

// smoke binds as the smoke SMPP account, submits one message and waits for its own DLR. ctx carries
// the overall deadline; retry is the interval between two refused binds (rotation takes a moment to
// reach session-manager).
func smoke(ctx context.Context, a *admin, dial dialFunc, retry time.Duration) error {
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

	msgID, err := submitSmoke(nc)
	if err != nil {
		return err
	}
	return waitForDLR(ctx, nc, msgID)
}

// rotateSmokeSecret finds the smoke account's active smpp_bind credential and rotates it, so the
// bind below never uses a secret the operator (or another test run) already saw.
func rotateSmokeSecret(ctx context.Context, a *admin, accountID string) (string, error) {
	var creds []credential
	if err := a.do(ctx, http.MethodGet, "/smpp-accounts/"+accountID+"/credentials", nil, &creds); err != nil {
		return "", err
	}
	var credID string
	for _, c := range creds {
		if c.SystemID == smokeSystemID && c.Status == "active" {
			credID = c.ID
			break
		}
	}
	if credID == "" {
		return "", fmt.Errorf("credential %q : introuvable ou inactive", smokeSystemID)
	}

	var rotated credentialSecret
	if err := a.do(ctx, http.MethodPost, "/smpp-accounts/"+accountID+"/credentials/"+credID+"/rotate", nil, &rotated); err != nil {
		return "", err
	}
	if rotated.Secret == "" {
		return "", fmt.Errorf("rotation de la credential %q : secret vide", smokeSystemID)
	}
	return rotated.Secret, nil
}

// bindWithRetry dials and binds until accepted or ctx is done. A refused bind — command_status other
// than OK — closes the connection and tries again after retry.
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

// submitSmoke sends the smoke message and returns the message id the SMSC assigned it.
func submitSmoke(nc net.Conn) (string, error) {
	req := smpp.PDU{Sequence: 2, Body: &smpp.SubmitSM{SMFields: smpp.SMFields{
		SourceAddrTON:      5,
		SourceAddr:         senderAddr,
		DestAddrTON:        1,
		DestAddrNPI:        1,
		DestinationAddr:    "33612345678",
		RegisteredDelivery: smpp.RegisteredDeliveryReceipt,
		ShortMessage:       []byte("step-275 smoke"),
	}}}
	if err := smpp.WritePDU(nc, req); err != nil {
		return "", fmt.Errorf("écriture submit_sm : %w", err)
	}
	resp, err := smpp.ReadPDU(nc)
	if err != nil {
		return "", fmt.Errorf("lecture submit_sm_resp : %w", err)
	}
	body, ok := resp.Body.(*smpp.SubmitSMResp)
	if !ok {
		return "", fmt.Errorf("réponse au submit_sm : %T", resp.Body)
	}
	if resp.Status != smpp.StatusOK {
		return "", fmt.Errorf("submit_sm refusé, command_status %#08x", resp.Status)
	}
	return body.MessageID, nil
}

// waitForDLR reads PDUs until it sees the deliver_sm carrying msgID's own DLR, answering
// enquire_link and every deliver_sm along the way, then unbinds. A deadline with no matching DLR is
// reported as an error naming the DLR, never as a bare read timeout.
func waitForDLR(ctx context.Context, nc net.Conn, msgID string) error {
	deadline, hasDeadline := ctx.Deadline()
	want := "id:" + msgID + " "
	seq := uint32(10)

	for {
		if hasDeadline {
			if err := nc.SetDeadline(deadline); err != nil {
				return fmt.Errorf("échéance de connexion : %w", err)
			}
		}
		pdu, err := smpp.ReadPDU(nc)
		if err != nil {
			return fmt.Errorf("aucun DLR pour %s avant l'échéance", msgID)
		}

		switch body := pdu.Body.(type) {
		case *smpp.EnquireLink:
			if err := smpp.WritePDU(nc, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.EnquireLinkResp{}}); err != nil {
				return fmt.Errorf("écriture enquire_link_resp : %w", err)
			}
		case *smpp.DeliverSM:
			if err := smpp.WritePDU(nc, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.DeliverSMResp{}}); err != nil {
				return fmt.Errorf("écriture deliver_sm_resp : %w", err)
			}
			if body.ESMClass&smpp.ESMClassMCDeliveryReceipt != 0 && strings.HasPrefix(string(body.ShortMessage), want) {
				seq++
				_ = smpp.WritePDU(nc, smpp.PDU{Sequence: seq, Body: &smpp.Unbind{}})
				return nil
			}
		}
	}
}
