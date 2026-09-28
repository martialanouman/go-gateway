package main

import "testing"

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
