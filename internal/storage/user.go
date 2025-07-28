package storage

import (
	"go-mini-server/core/db"
	"go-mini-server/core/db/pool"
	"go-mini-server/internal/user"
	"time"
)

type userRepository struct {
	pool pool.Service
}

func (a userRepository) Fetch(id int) (*user.User, error) {
	u := user.User{
		Entity:     db.Entity{Id: 0},
		Name:       "Name",
		SecondName: "Second",
		Position:   "CEO",
		HiringDate: time.Time{},
	}

	return &u, nil
}

func NewUserRepository(p pool.Service) user.Repository {
	return userRepository{pool: p}
}
