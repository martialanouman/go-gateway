package main

import (
	"strings"
	"testing"
)

func TestAdminTokenTakesTheFirstFieldOfTheFirstToken(t *testing.T) {
	got, err := adminToken("tok:admin:read|admin:write,t2:admin:read")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if got != "tok" {
		t.Errorf("adminToken = %q, want %q", got, "tok")
	}
}

func TestAdminTokenRejectsAnEmptyList(t *testing.T) {
	if _, err := adminToken(""); err == nil {
		t.Fatal("err = nil, want an error")
	}
}

func TestAdminTokenRejectsAnEmptyToken(t *testing.T) {
	if _, err := adminToken(":admin:read"); err == nil {
		t.Fatal("err = nil, want an error")
	}
}

func TestAdminTokenErrorDoesNotLeakTheOtherTokens(t *testing.T) {
	_, err := adminToken(":admin:read,secret-tok:admin:read")
	if err == nil {
		t.Fatal("err = nil, want an error")
	}
	if strings.Contains(err.Error(), "secret-tok") {
		t.Errorf("err = %q, ne doit pas contenir le jeton en clair", err.Error())
	}
}
