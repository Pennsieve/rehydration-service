package clients

import (
	"context"
	"io"
	"time"
)

type Client interface {
	Execute(context.Context, io.Reader, string, time.Duration) ([]byte, error)
}
