package web

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type (
	Config struct {
		Listen  string        `yaml:"listen"`
		Timeout time.Duration `yaml:"timeout"`
	}

	Handler func(r *http.Request) (interface{}, error)

	APIObject map[string]interface{}

	Middleware func(Handler) Handler
)

type Server struct {
	config     Config
	handlers   map[string]Handler
	middleware []Middleware
}

func NewServer(c Config, handlers map[string]Handler, middleware ...Middleware) *Server {
	return &Server{
		config:     c,
		handlers:   handlers,
		middleware: middleware,
	}
}

func (s *Server) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	log.Printf("request: %s %s\n", r.Method, r.URL.Path)
	defer recoverPanic(rw)

	rw.Header().Set("Content-Type", "application/json")

	h, err := s.findHandler(r.URL.Path)
	if err != nil {
		rw.WriteHeader(http.StatusNotFound)
		writeError(rw, "handler not found")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.config.Timeout)
	defer cancel()
	r = r.WithContext(ctx)

	finalHandler := wrapHandler(h, s.middleware...)

	result, err := finalHandler(r)
	if err != nil {
		writeError(rw, err.Error())
		return
	}

	if result == nil {
		writeJSON(rw, APIObject{"status": "success"})
		return
	}

	writeJSON(rw, APIObject{
		"status": "success",
		"data":   result,
	})
}

func (s *Server) findHandler(path string) (Handler, error) {
	if h, ok := s.handlers[path]; ok {
		return h, nil
	}
	return nil, fmt.Errorf("handler not found for path %s", path)
}

func LoggerMiddleware(next Handler) Handler {
	return func(r *http.Request) (interface{}, error) {
		start := time.Now()
		res, err := next(r)
		log.Printf("Handled %s in %s", r.URL.Path, time.Since(start))
		return res, err
	}
}

func wrapHandler(h Handler, middleware ...Middleware) Handler {
	for i := len(middleware) - 1; i >= 0; i-- {
		h = middleware[i](h)
	}
	return h
}

func writeJSON(rw http.ResponseWriter, data interface{}) {
	_ = json.NewEncoder(rw).Encode(data)
}

func writeError(rw http.ResponseWriter, err string) {
	_ = json.NewEncoder(rw).Encode(APIObject{
		"status": "error",
		"error":  err,
	})
}

func recoverPanic(rw http.ResponseWriter) {
	if r := recover(); r != nil {
		log.Printf("Recovered from panic: %v", r)
		writeError(rw, "internal server error")
	}
}
