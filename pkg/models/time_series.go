package models

type SeriesIterator interface {
	HasNext() bool
	Next() []byte
}

type series struct {
	i         int
	key       []byte
	fieldKeys [][]byte
	valueBuf  []byte
}

func (s *series) HasNext() bool {
	return s.i < len(s.fieldKeys)
}

func (s *series) Next() []byte {
	s.valueBuf = s.valueBuf[:0]
	s.valueBuf = append(s.valueBuf, s.key...)
	s.valueBuf = append(s.valueBuf, s.fieldKeys[s.i]...)
	s.i++
	return s.valueBuf
}
