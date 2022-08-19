package task

type Option func(*Message)

func WithTTl(ttl int64) Option {
	if ttl <= 0 {
		panic("ttl must be greater than 0")
	}

	return func(m *Message) {
		m.TTl = ttl
	}
}

func WithCallback(callback interface{}) Option {
	return func(m *Message) {
		m.Callback = callback
	}
}

func WithRetry(retry int) Option {
	if retry <= 0 {
		panic("retry must be greater than 0")
	}

	return func(m *Message) {
		m.Retry = retry
	}
}
