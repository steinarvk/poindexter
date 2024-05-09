package poindexterdb

type requestOptions struct {
	debug bool
}

type RequestOption func(*requestOptions) error

func WithDebug() RequestOption {
	return func(o *requestOptions) error {
		o.debug = true
		return nil
	}
}
