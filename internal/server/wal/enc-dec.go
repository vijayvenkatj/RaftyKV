package wal

import "encoding/binary"

func encode(e *LogEntry) []byte {
	op := []byte(e.Operation)
	key := []byte(e.Key)
	val := []byte(e.Value)

	total := 12 + len(op) + len(key) + len(val)
	buf := make([]byte, total)

	// write lengths
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(op)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(val)))

	offset := 12

	copy(buf[offset:], op)
	offset += len(op)

	copy(buf[offset:], key)
	offset += len(key)

	copy(buf[offset:], val)

	return buf
}
func decode(data []byte) (*LogEntry, error) {
	offset := 0

	opLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	keyLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	valLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	op := string(data[offset : offset+int(opLen)])
	offset += int(opLen)

	key := string(data[offset : offset+int(keyLen)])
	offset += int(keyLen)

	value := string(data[offset : offset+int(valLen)])

	return &LogEntry{
		Operation: op,
		Key:       key,
		Value:     value,
	}, nil
}
