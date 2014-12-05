package fluent

import (
	"container/list"
	"math"
	"net"
	"sync"
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
	list     *list.List
	limit    int
	m        sync.Mutex
	flushing bool
}

func newPending(limit int) *pending {
	return &pending{
		list:  list.New(),
		limit: limit,
	}
}

func (p *pending) Add(b []byte) {
	p.m.Lock()
	defer p.m.Unlock()

	// If not flushing now, trim the pending if limit exceeded.
	// Otherwise, to avoid conflict, trimming is temporary disabled.
	if !p.flushing {
		for i := p.list.Len() - p.limit; i >= 0; i-- {
			p.list.Remove(p.list.Front())
		}
	}

	p.list.PushBack(b)
}

func (p *pending) Flush(conn net.Conn, timeout time.Duration) error {
	p.m.Lock()
	n := p.list.Len()
	p.flushing = true
	p.m.Unlock()

	defer func() {
		p.flushing = false
	}()

	for i, e := 0, p.list.Front(); i < n; i, e = i+1, p.list.Front() {
		conn.SetDeadline(time.Now().Add(timeout))
		if _, err := conn.Write(e.Value.([]byte)); err != nil {
			return err
		}

		p.m.Lock()
		p.list.Remove(e)
		p.m.Unlock()
	}
	conn.SetDeadline(time.Time{})
	return nil
}

type Options struct {
	Addr           string
	TagPrefix      string
	SendBufSize    int
	ErrorBufSize   int
	ErrorHandler   ErrorHandler
	LogHandler     func(string, ...interface{})
	DialTimeout    time.Duration
	CloseTimeout   time.Duration
	Timeout        time.Duration
	RetryWait      time.Duration
	MaxBackoff     int
	MaxPendingSize int
	IsFluxion      bool

	// If NonBlocking is true and send buffer is full, further events will be dropped.
	NonBlocking bool
}

func (o *Options) Default() {
	if o.SendBufSize == 0 {
		o.SendBufSize = 50000
	}
	if o.ErrorBufSize == 0 {
		o.ErrorBufSize = 100
	}
	if o.DialTimeout == 0 {
		o.DialTimeout = 5 * time.Second
	}
	if o.CloseTimeout == 0 {
		o.CloseTimeout = 5 * time.Second
	}
	if o.Timeout == 0 {
		o.Timeout = 5 * time.Second
	}
	if o.RetryWait == 0 {
		o.RetryWait = 100 * time.Millisecond
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
	mh      *codec.MsgpackHandle
}

func NewClient(opts Options) (*Client, error) {
	opts.Default()
	c := &Client{
		opts:    &opts,
		inputCh: make(chan interface{}, opts.SendBufSize),
		closeCh: make(chan bool),
		pending: newPending(opts.MaxPendingSize),
	}
	c.log("Fluent options: %+v", opts)
	if !opts.IsFluxion {
		c.mh = &codec.MsgpackHandle{}
	} else {
		c.mh = &codec.MsgpackHandle{RawToString: true, WriteExt: true}
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

	go c.sender()
	return c, nil
}

// Send sends record v with given tag. v can be any type which can be encoded
// to msgpack. Specially, if type of v is []byte, it will be sent without any
// modification.
func (c *Client) Send(tag string, v interface{}) {
	c.SendWithTime(tag, time.Now(), v)
}

// SendWithTime sends record v with given tag and time. See Send.
func (c *Client) SendWithTime(tag string, t time.Time, v interface{}) {
	if c.opts.TagPrefix != "" {
		if tag != "" {
			tag = c.opts.TagPrefix + "." + tag
		} else {
			tag = c.opts.TagPrefix
		}
	}

	var tt interface{}
	if !c.opts.IsFluxion {
		tt = t.Unix()
	} else {
		tt = t
	}

	val := []interface{}{tag, tt, v}
	if c.opts.NonBlocking {
		select {
		case c.inputCh <- val:
		default:
		}
	} else {
		c.inputCh <- val
	}
}

func (c *Client) Close() {
	close(c.inputCh)

	select {
	case <-c.closeCh:
	case <-time.After(c.opts.CloseTimeout):
	}
}

func (c *Client) sender() {
	defer func() {
		if c.errorCh != nil {
			close(c.errorCh)
		} else {
			close(c.closeCh)
		}
	}()

	for v := range c.inputCh {
		b, err := c.encode(v)
		if err != nil {
			c.pushError(err)
			continue
		}
		c.conn.SetDeadline(time.Now().Add(c.opts.Timeout))
		if _, err = c.conn.Write(b); err != nil {
			c.pending.Add(b)
			c.pushError(err)
			c.conn.Close()
			c.reconnect()
		}
		// Cancel deadline setting
		c.conn.SetDeadline(time.Time{})
	}
}

func (c *Client) encode(v interface{}) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	var b []byte
	err := codec.NewEncoderBytes(&b, c.mh).Encode(v)
	return b, err
}

func (c *Client) reconnect() {
	c.log("Connection failed, enter reconnection loop")
	stop := c.poll()
	for {
		for attempts := 0; ; attempts++ {
			time.Sleep(backoff(c.opts.RetryWait, attempts, c.opts.MaxBackoff))
			c.log("Reconnect attempt %d", attempts+1)
			if conn, err := net.DialTimeout("tcp", c.opts.Addr, c.opts.DialTimeout); err == nil {
				c.log("Reconnected! try to flush pendings")
				err, f := c.flush(conn, stop)
				if err == nil {
					c.conn = conn
					c.log("Reconnection process completed! enter normal loop")
					return
				}
				conn.Close()
				if f != nil {
					stop = f
				}
			}
		}
	}
}

func (c *Client) flush(conn net.Conn, stop func()) (error, func()) {
	for attempts := 1; ; attempts++ {
		t := time.Now()
		if err := c.pending.Flush(conn, c.opts.Timeout); err != nil {
			c.log("Failed to flush pendings, retrying...")
			return err, nil
		}
		if took := time.Since(t); took < c.opts.Timeout {
			c.log("Flushing almost completed in an acceptable time (%v)", took)
			stop()
			break
		} else {
			c.log("Flush attempts %d in %v", attempts, took)
		}
	}
	if err := c.pending.Flush(conn, c.opts.Timeout); err != nil {
		c.log("Failed to flush last piece of pendings")
		f := c.poll()
		return err, f
	}
	c.log("All flushing process succeeded!")
	return nil, nil
}

func (c *Client) poll() (stop func()) {
	closeC, doneC := make(chan bool, 1), make(chan bool)
	go func() {
		c.log("Start pending loop")
		defer c.log("Stop pending loop")
		for {
			select {
			case <-closeC:
				doneC <- true
				return
			case v, ok := <-c.inputCh:
				if !ok {
					doneC <- true
					return
				}

				b, err := c.encode(v)
				if err != nil {
					c.pushError(err)
				} else {
					c.pending.Add(b)
				}
			}
		}
	}()
	return func() {
		closeC <- true
		<-doneC
	}
}

func (c *Client) log(s string, args ...interface{}) {
	if c.opts.LogHandler != nil {
		c.opts.LogHandler(s, args...)
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
