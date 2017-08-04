package resp


// @author  Mikhail Kirillov <mikkirillov@yandex.ru>
// @version 1.000
// @date    2017-08-04


import (
  "fmt"
  "reflect"
  "strconv"
)


func Encode(args ...interface{}) ([]byte, error) {

  argsNum := len(args)
  buf := make([]byte, 0, 10*argsNum)
  buf = append(buf, '*')
  buf = strconv.AppendInt(buf, int64(argsNum), 10)
  buf = addTail(buf)

  for _, arg := range args {
    switch v := arg.(type) {

    case nil:
      buf = addBytes(buf, []byte{})

    case bool:
      if v {
        buf = addBytes(buf, []byte{'1'})
      } else {
        buf = addBytes(buf, []byte{'0'})
      }

    case []byte:
      buf = addBytes(buf, v)

    case string:
      buf = addBytes(buf, []byte(v))

    case int:
      buf = addInt64(buf, int64(v))
    case int8:
      buf = addInt64(buf, int64(v))
    case int16:
      buf = addInt64(buf, int64(v))
    case int32:
      buf = addInt64(buf, int64(v))
    case int64:
      buf = addInt64(buf, v)
    case uint:
      buf = addInt64(buf, int64(v))
    case uint8:
      buf = addInt64(buf, int64(v))
    case uint16:
      buf = addInt64(buf, int64(v))
    case uint32:
      buf = addInt64(buf, int64(v))
    case uint64:
      buf = addInt64(buf, int64(v))

    case float32:
      buf = addFloat(buf, float64(v))
    case float64:
      buf = addFloat(buf, v)

    default:
      return nil, fmt.Errorf("Invalid argument type : {%s}, when pack command.", reflect.TypeOf(arg))
    }
  }

  return buf, nil
}


func intLen(number int64) int64 {
  var count int64 = 1
  if number < 0 {
    number = -number
    count = 2
  }
  for number > 9 {
    number /= 10
    count++
  }
  return count
}


func addTail(buf []byte) []byte {
  return append(buf, '\r', '\n')
}


func addInt64(buf []byte, n int64) []byte {
  buf = append(buf, '$')
  buf = strconv.AppendInt(buf, intLen(n), 10)
  buf = addTail(buf)
  buf = strconv.AppendInt(buf, n, 10)
  return addTail(buf)
}


func addBytes(buf []byte, b []byte) []byte {
  buf = append(buf, '$')
  buf = strconv.AppendInt(buf, int64(len(b)), 10)
  buf = addTail(buf)
  buf = append(buf, b...)
  return addTail(buf)
}


func addFloat(buf []byte, f float64) []byte {
  return addBytes(buf, []byte(strconv.FormatFloat(f, 'f', -1, 64)))
}

