package fluent

import (
	"container/list"
	"io"
	"math"
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

type pending struct {
	list  *list.List
	limit int
}

func newPending(limit int) *pending {
	return &pending{
		list:  list.New(),
		limit: limit,
	}
}

func (p *pending) Add(b []byte) {
	// trim the pending if limit exceeded
	for i := p.list.Len() - p.limit; i >= 0; i-- {
		p.list.Remove(p.list.Front())
	}

	p.list.PushBack(b)
}

func (p *pending) Flush(w io.Writer) error {
	for e := p.list.Front(); e != nil; e = p.list.Front() {
		if _, err := w.Write(e.Value.([]byte)); err != nil {
			return err
		}
		p.list.Remove(e)
	}
	return nil
}

type Options struct {
	Addr           string
	TagPrefix      string
	SendBufSize    int
	ErrorBufSize   int
	ErrorHandler   ErrorHandler
	DialTimeout    time.Duration
	CloseTimeout   time.Duration
	RetryInterval  time.Duration
	MaxBackoff     int
	MaxPendingSize int
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
	if o.RetryInterval == 0 {
		o.RetryInterval = 100 * time.Millisecond
	}
	if o.MaxBackoff == 0 {
		o.MaxBackoff = 8
	}
	if o.MaxPendingSize == 0 {
		o.MaxPendingSize = 1000 * 1000
	}
}

type Client struct {
	conn    net.Conn
	inputCh chan interface{}
	errorCh chan error
	closeCh chan bool
	opts    *Options
	pending *pending
}

func NewClient(opts Options) (*Client, error) {
	opts.Default()
	c := &Client{
		opts:    &opts,
		inputCh: make(chan interface{}, opts.SendBufSize),
		closeCh: make(chan bool),
		pending: newPending(opts.MaxPendingSize),
	}

	conn, err := net.DialTimeout("tcp", c.opts.Addr, c.opts.DialTimeout)
	if err != nil {
		return nil, err
	}
	c.conn = conn

	if c.opts.ErrorHandler != nil {
		c.errorCh = make(chan error, c.opts.ErrorBufSize)
		go c.errorWorker()
	}

	go c.worker()
	return c, nil
}

func (c *Client) Send(tag string, v interface{}) {
	if c.opts.TagPrefix != "" {
		if tag != "" {
			tag = c.opts.TagPrefix + "." + tag
		} else {
			tag = c.opts.TagPrefix
		}
	}

	now := time.Now().Unix()
	val := []interface{}{tag, now, v}
	c.inputCh <- val
}

func (c *Client) Close() {
	close(c.inputCh)

	select {
	case <-c.closeCh:
	case <-time.After(c.opts.CloseTimeout):
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

	for v := range c.inputCh {
		var b []byte
		if err := codec.NewEncoderBytes(&b, &codec.MsgpackHandle{}).Encode(v); err != nil {
			c.pushError(err)
			continue
		}

		if _, err := c.conn.Write(b); err != nil {
			c.pending.Add(b)
			c.pushError(err)
			c.reconnect()
		}
	}
}

func (c *Client) reconnect() {
	attempts := 0
	retry := time.After(0)

	for {
		select {
		case v, ok := <-c.inputCh:
			if !ok {
				return
			}

			var b []byte
			if err := codec.NewEncoderBytes(&b, &codec.MsgpackHandle{}).Encode(v); err != nil {
				c.pushError(err)
				continue
			}
			c.pending.Add(b)
		case <-retry:
			if conn, err := net.DialTimeout("tcp", c.opts.Addr, c.opts.DialTimeout); err == nil {
				if err := c.pending.Flush(conn); err == nil {
					c.conn = conn
					return
				}
			}
			retry = time.After(backoff(c.opts.RetryInterval, attempts, c.opts.MaxBackoff))
			attempts++
		}
	}
}

func (c *Client) errorWorker() {
	defer close(c.closeCh)

	for err := range c.errorCh {
		c.opts.ErrorHandler.HandleError(err)
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

func backoff(interval time.Duration, count, limit int) time.Duration {
	if count > limit {
		count = limit
	}
	return interval * time.Duration(math.Exp2(float64(count)))
}
