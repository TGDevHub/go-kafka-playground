package pool

type Service interface {
}

type service struct {
	config Config
	pool   Pool
}

func NewService(c Config) Service {
	return &service{
		config: c,
	}
}
