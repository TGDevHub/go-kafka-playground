package pool

type poolConnectionHandler struct {
	instance *pool
}

func (pc *poolConnectionHandler) Close() {
}

func (pc *poolConnectionHandler) LoadOne() (map[string]interface{}, error) {
	return nil, nil
}
