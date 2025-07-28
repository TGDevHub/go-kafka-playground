package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-mini-server/internal/web"
)

func TestHandleUserGet(t *testing.T) {
	c, err := loadConfig()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	initServices(c)

	handlers := map[string]web.Handler{
		"/user/get": handleUserGet,
	}

	server := web.NewServer(web.Config{Timeout: 2 * time.Second}, handlers)

	body := bytes.NewBufferString(`{"id": 0}`)
	req := httptest.NewRequest(http.MethodPost, "/user/get", body)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	fmt.Println(response)

	if response["status"] != "success" {
		t.Errorf("unexpected status: %v", response["status"])
	}

	data, ok := response["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("data is not a map")
	}

	if data["name"] != "Name" || data["position"] != "CEO" {
		t.Errorf("unexpected user data: %+v", data)
	}
}
