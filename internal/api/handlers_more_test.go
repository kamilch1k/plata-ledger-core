package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetAccount_200(t *testing.T) {
	h := NewServer(newFake())
	rr := do(t, h, "GET", "/accounts/acc_1", nil, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rr.Code)
	}
}

func TestStatement_200(t *testing.T) {
	h := NewServer(newFake())
	rr := do(t, h, "GET", "/accounts/acc_1/statement?limit=10", nil, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rr.Code)
	}
}

func TestHealth_200(t *testing.T) {
	h := NewServer(newFake())
	rr := do(t, h, "GET", "/health", nil, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rr.Code)
	}
}

func TestCreateAccount_BadBody_400(t *testing.T) {
	h := NewServer(newFake())
	req := httptest.NewRequest("POST", "/accounts", strings.NewReader("{not valid json"))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", rr.Code)
	}
}

func TestCreateAccount_InvalidCurrency_400(t *testing.T) {
	h := NewServer(newFake())
	rr := do(t, h, "POST", "/accounts", nil,
		map[string]any{"name": "X", "currency": "bad", "initial_balance_minor": 0})
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", rr.Code)
	}
}
