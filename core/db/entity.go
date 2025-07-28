package db

type Entity struct {
	Id int `db:"id" json:"id"`
}

func (e *Entity) GetID() int {
	return e.Id
}
