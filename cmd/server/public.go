package main

import (
	"encoding/json"
	"go-mini-server/internal/errors"
	"io"
	"log"
	"net/http"
	"strconv"
)

func handleUserGet(r *http.Request) (interface{}, error) {
	body := struct {
		Id int `json:"id"`
	}{}

	err := decodeBody(r, &body)
	if err != nil {
		return nil, errors.InvalidRequest
	}

	publishEvent(body.Id)

	u, err := userService.FetchById(body.Id)
	if err != nil {
		return nil, err
	}

	if producer != nil {
		_ = producer.Send("user", "user.get called")
	}
	return u, err
}

func publishEvent(id int) {
	err := producer.Produce("user_"+strconv.Itoa(id), "Hello, Kafka!")
	if err != nil {
		log.Printf("Produce error: %v", err)
	}
}

func decodeBody(r *http.Request, v interface{}) error {
	if r.Method != http.MethodPost {
		return errors.InvalidRequest
	}

	body, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(body, &v); err != nil {
		return errors.InvalidRequest
	}

	return nil
}
