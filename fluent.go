package fluent

import (
	"net"
	"time"

	"github.com/ugorji/go/codec"
)

type ErrorHandler interface {
	HandleError(error)
}

type ErrorHandlerFunc func(error)

func (f ErrorHandlerFunc) HandleError(err error) {
	f(err)
}

type Options struct {
	SendBufSize  int
	ErrorBufSize int
	ErrorHandler ErrorHandler
	DialTimeout  time.Duration
	CloseTimeout time.Duration
}

func (o *Options) Default() {
	if o.SendBufSize < 0 {
		o.SendBufSize = 100
	}
	if o.ErrorBufSize < 0 {
		o.ErrorBufSize = 100
	}
	if o.DialTimeout == 0 {
		o.DialTimeout = 5 * time.Second
	}
	if o.CloseTimeout == 0 {
		o.CloseTimeout = 5 * time.Second
	}
}

type Client struct {
	conn         net.Conn
	inputCh      chan interface{}
	errorCh      chan error
	closeCh      chan bool
	errorHandler ErrorHandler
	closeTimeout time.Duration
}

func NewClient(addr string, opts Options) (*Client, error) {
	opts.Default()
	c := &Client{
		inputCh:      make(chan interface{}, opts.SendBufSize),
		closeCh:      make(chan bool),
		closeTimeout: opts.CloseTimeout,
	}

	conn, err := net.DialTimeout("tcp", addr, opts.DialTimeout)
	if err != nil {
		return nil, err
	}
	c.conn = conn

	if opts.ErrorHandler != nil {
		c.errorCh = make(chan error, opts.ErrorBufSize)
		c.errorHandler = opts.ErrorHandler
		go c.errorWorker()
	}

	go c.worker()
	return c, nil
}

func (c *Client) Send(tag string, v interface{}) {
	now := time.Now().Unix()
	val := []interface{}{tag, now, v}
	c.inputCh <- val
}

func (c *Client) Close() {
	close(c.inputCh)

	select {
	case <-c.closeCh:
	case <-time.After(c.closeTimeout):
	}
}

func (c *Client) worker() {
	defer func() {
		if c.errorCh != nil {
			close(c.errorCh)
		} else {
			close(c.closeCh)
		}
	}()

	var b []byte
	for v := range c.inputCh {
		if err := codec.NewEncoderBytes(&b, &codec.MsgpackHandle{}).Encode(v); err != nil {
			c.pushError(err)
			continue
		}

		if _, err := c.conn.Write(b); err != nil {
			c.pushError(err)
		}
	}
}

func (c *Client) errorWorker() {
	defer close(c.closeCh)

	for err := range c.errorCh {
		c.errorHandler.HandleError(err)
	}
}

func (c *Client) pushError(err error) {
	if c.errorCh == nil {
		return
	}

	select {
	case c.errorCh <- err:
	default:
	}
}
