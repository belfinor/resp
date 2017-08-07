package resp


import (
  "bufio"
  "bytes"
  "errors"
  "fmt"
  "io"
  "net"
  "strconv"
  "sync"
  "time"
)


type Conn struct {

  pending int
  err     error
  addr    string
  conn    net.Conn

  // Read
  readTimeout time.Duration
  br          *bufio.Reader

  // Write
  writeTimeout time.Duration
  bw           *bufio.Writer

  // Scratch space for formatting argument length.
  // '*' or '$', length, "\r\n"
  lenScratch [32]byte

  // Scratch space for formatting integers and floats.
  numScratch [40]byte

  sync.Mutex
}


func Dial(address string) (*Conn, error) {

  netConn, err := net.Dial( "tcp", address )
  if err != nil {
    return nil, err
  }

  conn := NewConn(netConn)
  conn.addr = address
  
  return conn, nil
}


func NewConn(netConn net.Conn) *Conn {
  return &Conn{
    conn:         netConn,
    bw:           bufio.NewWriter(netConn),
    br:           bufio.NewReader(netConn),
    readTimeout:  30 * time.Second,
    writeTimeout: 5 * time.Second,
  }
}


func (c *Conn) Close() error {
  c.Lock()
  err := c.err
  if c.err == nil {
    c.err = errors.New("redigo: closed")
    err = c.conn.Close()
  }
  c.Unlock()
  return err
}

func (c *Conn) fatal(err error) error {
  c.Lock()
  if c.err == nil {
    c.err = err
    c.conn.Close()
  }
  c.Unlock()
  return err
}

func (c *Conn) Err() error {
  c.Lock()
  err := c.err
  c.Unlock()
  return err
}

func (c *Conn) writeLen(prefix byte, n int) error {
  c.lenScratch[len(c.lenScratch)-1] = '\n'
  c.lenScratch[len(c.lenScratch)-2] = '\r'
  i := len(c.lenScratch) - 3
  for {
    c.lenScratch[i] = byte('0' + n%10)
    i -= 1
    n = n / 10
    if n == 0 {
      break
    }
  }
  c.lenScratch[i] = prefix
  _, err := c.bw.Write(c.lenScratch[i:])
  return err
}


func (c *Conn) writeString(s string) error {
  c.writeLen('$', len(s))
  c.bw.WriteString(s)
  _, err := c.bw.WriteString("\r\n")
  return err
}


func (c *Conn) writeBytes(p []byte) error {
  c.writeLen('$', len(p))
  c.bw.Write(p)
  _, err := c.bw.WriteString("\r\n")
  return err
}


func (c *Conn) writeIntBytes(p []byte) error {
  c.writeLen(':', len(p))
  c.bw.Write(p)
  _, err := c.bw.WriteString("\r\n")
  return err
}


func (c *Conn) writeInt64(n int64) error {
  return c.writeIntBytes(strconv.AppendInt(c.numScratch[:0], n, 10))
}


func (c *Conn) writeFloat64(n float64) error {
  return c.writeBytes(strconv.AppendFloat(c.numScratch[:0], n, 'g', -1, 64))
}


func (c *Conn) writeCommand(cmd string, args []interface{}) (err error) {
  c.writeLen('*', 1+len(args))
  err = c.writeString(cmd)
  for _, arg := range args {
    if err != nil {
      break
    }
    switch arg := arg.(type) {
    case string:
      err = c.writeString(arg)
    case []byte:
      err = c.writeBytes(arg)
    case int:
      err = c.writeInt64(int64(arg))
    case int64:
      err = c.writeInt64(arg)
    case float64:
      err = c.writeFloat64(arg)
    case bool:
      if arg {
        err = c.writeString("1")
      } else {
        err = c.writeString("0")
      }
    case nil:
      err = c.writeString("")
    default:
      var buf bytes.Buffer
      fmt.Fprint(&buf, arg)
      err = c.writeBytes(buf.Bytes())
    }
  }
  return err
}

type protocolError string

func (pe protocolError) Error() string {
  return fmt.Sprintf("redigo: %s (possible server error or unsupported concurrent read by application)", string(pe))
}

func (c *Conn) readLine() ([]byte, error) {
  p, err := c.br.ReadSlice('\n')
  if err == bufio.ErrBufferFull {
    return nil, protocolError("long response line")
  }
  if err != nil {
    return nil, err
  }
  i := len(p) - 2
  if i < 0 || p[i] != '\r' {
    return nil, protocolError("bad response line terminator")
  }
  return p[:i], nil
}

// parseLen parses bulk string and array lengths.
func parseLen(p []byte) (int, error) {
  if len(p) == 0 {
    return -1, protocolError("malformed length")
  }

  if p[0] == '-' && len(p) == 2 && p[1] == '1' {
    // handle $-1 and $-1 null replies.
    return -1, nil
  }

  var n int
  for _, b := range p {
    n *= 10
    if b < '0' || b > '9' {
      return -1, protocolError("illegal bytes in length")
    }
    n += int(b - '0')
  }

  return n, nil
}

// parseInt parses an integer reply.
func parseInt(p []byte) (interface{}, error) {
  if len(p) == 0 {
    return 0, protocolError("malformed integer")
  }

  var negate bool
  if p[0] == '-' {
    negate = true
    p = p[1:]
    if len(p) == 0 {
      return 0, protocolError("malformed integer")
    }
  }

  var n int64
  for _, b := range p {
    n *= 10
    if b < '0' || b > '9' {
      return 0, protocolError("illegal bytes in length")
    }
    n += int64(b - '0')
  }

  if negate {
    n = -n
  }
  return n, nil
}


var (
  okReply   interface{} = "OK"
  pongReply interface{} = "PONG"
)


func (c *Conn) readReply() (interface{}, error) {
  line, err := c.readLine()
  if err != nil {
    return nil, err
  }
  if len(line) == 0 {
    return nil, protocolError("short response line")
  }
  switch line[0] {
  case '+':
    switch {
    case len(line) == 3 && line[1] == 'O' && line[2] == 'K':
      return okReply, nil
    case len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G':
      return pongReply, nil
    default:
      return string(line[1:]), nil
    }
  case '-':
    return errors.New(string(line[1:])), nil
  case ':':
    return parseInt(line[1:])
  case '$':
    n, err := parseLen(line[1:])
    if n < 0 || err != nil {
      return nil, err
    }
    p := make([]byte, n)
    _, err = io.ReadFull(c.br, p)
    if err != nil {
      return nil, err
    }
    if line, err := c.readLine(); err != nil {
      return nil, err
    } else if len(line) != 0 {
      return nil, protocolError("bad bulk string format")
    }
    return p, nil
  case '*':
    n, err := parseLen(line[1:])
    if n < 0 || err != nil {
      return nil, err
    }
    r := make([]interface{}, n)
    for i := range r {
      r[i], err = c.readReply()
      if err != nil {
        return nil, err
      }
    }
    return r, nil
  }
  return nil, protocolError("unexpected response line")
}

func (c *Conn) Send(cmd string, args ...interface{}) error {
  c.Lock()
  c.pending += 1
  c.Unlock()
  if c.writeTimeout != 0 {
    c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
  }
  if err := c.writeCommand(cmd, args); err != nil {
    return c.fatal(err)
  }
  return nil
}


func (c *Conn) SendStatus(status string) error {
  if c.writeTimeout != 0 {
    c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
  }

  _, err := c.bw.Write( []byte( "+" + status + "\r\n" ) )
  if err == nil {
    c.bw.Flush()
  }

  return err
}


func (c *Conn) SendError(err error) error {
  if c.writeTimeout != 0 {
    c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
  }

  _, err = c.bw.Write( []byte( "-" + err.Error() + "\r\n" ) )
  if err == nil {
    c.bw.Flush()
  }

  return err
}


func (c *Conn) Flush() error {
  if c.writeTimeout != 0 {
    c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
  }
  if err := c.bw.Flush(); err != nil {
    return c.fatal(err)
  }
  return nil
}

func (c *Conn) Receive() (reply interface{}, err error) {
  if c.readTimeout != 0 {
    c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
  }
  if reply, err = c.readReply(); err != nil {
    return nil, c.fatal(err)
  }
  
  c.Lock()
  if c.pending > 0 {
    c.pending -= 1
  }
  c.Unlock()
  if err, ok := reply.(error); ok {
    return nil, err
  }
  return
}


func (c *Conn) Do(cmd string, args ...interface{}) (interface{}, error) {
  c.Lock()
  pending := c.pending
  c.pending = 0
  c.Unlock()

  if cmd == "" && pending == 0 {
    return nil, nil
  }

  if c.writeTimeout != 0 {
    c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
  }

  if cmd != "" {
    if err := c.writeCommand(cmd, args); err != nil {
      return nil, c.fatal(err)
    }
  }

  if err := c.bw.Flush(); err != nil {
    return nil, c.fatal(err)
  }

  if c.readTimeout != 0 {
    c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
  }

  if cmd == "" {
    reply := make([]interface{}, pending)
    for i := range reply {
      r, e := c.readReply()
      if e != nil {
        return nil, c.fatal(e)
      }
      reply[i] = r
    }
    return reply, nil
  }

  var err error
  var reply interface{}
  for i := 0; i <= pending; i++ {
    var e error
    if reply, e = c.readReply(); e != nil {
      return nil, c.fatal(e)
    }
    if e, ok := reply.(error); ok && err == nil {
      err = e
    }
  }
  return reply, err
}

