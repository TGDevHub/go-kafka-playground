package user

import (
	"go-mini-server/core/db"
	"time"
)

type User struct {
	db.Entity
	Name       string    `db:"name" json:"name"`
	SecondName string    `db:"second_name" json:"second_name"`
	Position   string    `db:"position" json:"position"`
	HiringDate time.Time `db:"hiring_date" json:"hiring_date"`
}
