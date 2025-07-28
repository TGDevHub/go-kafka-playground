package user

type Repository interface {
	Fetch(id int) (*User, error)
}
