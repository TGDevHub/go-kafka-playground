package user

type Service interface {
	FetchById(id int) (*User, error)
}

type service struct {
	repo Repository
}

func (s *service) FetchById(id int) (*User, error) {
	u, err := s.repo.Fetch(id)
	if err != nil {
		return nil, err
	}

	return u, nil
}

func NewService(r Repository) Service {
	return &service{
		repo: r,
	}
}
