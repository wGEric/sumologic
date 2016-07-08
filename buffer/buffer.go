package buffer

import (
	"bytes"
	"sync"

	"github.com/nutmegdevelopment/sumologic/upload"
)

type sender interface {
	send([]byte, string) error
}

// Buffer is a basic buffer structure.
type Buffer struct {
	sync.Mutex
	data  [][]byte
	names []string
	ref   int
	size  int
}

// NewBuffer allocates a new buffer
func NewBuffer(size int) *Buffer {
	b := new(Buffer)
	b.data = make([][]byte, size)
	b.names = make([]string, size)
	b.size = size
	return b
}

// Add appends data to the buffer
func (b *Buffer) Add(data []byte, name string) {
	b.Lock()
	defer b.Unlock()
	if b.ref >= len(b.data) {
		b.data = append(b.data, make([][]byte, b.size)...)
		b.names = append(b.names, make([]string, b.size)...)
	}
	b.data[b.ref] = data
	b.names[b.ref] = name
	b.ref++
}

// Send transmits data in the buffer using a provided sendFunc.
// This is not safe to call concurrently.
func (b *Buffer) Send(u upload.Uploader) (err error) {
	b.Lock()
	buf := b.data[:b.ref]
	nbuf := b.names[:b.ref]
	b.Unlock()

	packets := make(map[string][]byte)

	for i, n := range nbuf {
		if _, ok := packets[n]; ok {
			packets[n] = bytes.Join([][]byte{packets[n], buf[i]}, []byte("\n"))
		} else {
			packets[n] = buf[i]
		}
	}

	for n := range packets {
		thisErr := u.Send(packets[n], n)
		if thisErr != nil {
			err = thisErr
		}
	}
	if err == nil {
		// If all uploads suceeded, clear transmitted portion of buffer
		b.Lock()
		b.data = b.data[len(buf):]
		b.names = b.names[len(nbuf):]
		b.ref = b.ref - len(buf)
		b.Unlock()
	}

	return
}
