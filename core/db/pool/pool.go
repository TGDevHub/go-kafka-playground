package pool

import (
	"log"
	"sync"
)

type (
	Pool interface {
	}

	pool struct {
		index int
		log   log.Logger
		mutex sync.Mutex
	}
)
