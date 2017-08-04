package resp


// @author  Mikhail Kirillov <mikkirillov@yandex.ru>
// @version 1.000
// @date    2017-08-04


import (
  "bufio"
  "errors"
  "io"
  "strconv"
)


func DecodeBufio(r *bufio.Reader) (*Message, error) {
  line, e := r.ReadBytes('\n')
  if e != nil {
    return nil, e
  }

  if len(line) <= 2 {
    return nil, nil
  }

  line = line[:len(line)-2]
  switch line[0] {

  case MessageError:
    return &Message{
      Type:  MessageError,
      Error: errors.New(string(line[1:])),
    }, nil

  case MessageStatus:
    return &Message{
      Type:   MessageStatus,
      Status: string(line[1:]),
    }, nil

  case MessageInt:
    n, err := strconv.ParseInt(string(line[1:]), 10, 64)
    if err != nil {
      return nil, err
    }

    return &Message{
      Type:    MessageInt,
      Integer: n,
    }, nil

  case MessageBulk:
    l, err := strconv.Atoi(string(line[1:]))
    if err != nil {
      return nil, err
    }

    if l < 0 {
      return &Message{
        Bulk: nil,
        Type: MessageBulk,
      }, nil
    }

    buf := make([]byte, l+2)
    if _, err := io.ReadFull(r, buf); err != nil {
      return nil, err
    }
    return &Message{
      Bulk: buf[:l],
      Type: MessageBulk,
    }, nil

  case MessageMulti:
    l, e := strconv.Atoi(string(line[1:]))
    if e != nil {
      return nil, e
    }

    if l < 0 {
      return &Message{Type: MessageMulti}, nil
    }
    ret := make([]*Message, l)
    for i := 0; i < l; i++ {
      m, err := DecodeBufio(r)
      if err != nil {
        return nil, err
      }
      ret[i] = m
    }
    return &Message{
      Type:  MessageMulti,
      Multi: ret,
    }, nil
  }
  return nil, errors.New("Received illegal data from redis.")
}

