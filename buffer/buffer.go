package buffer // import "github.com/nutmegdevelopment/sumologic/buffer"

import (
	"bytes"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/nutmegdevelopment/sumologic/upload"
)

// DebugLogging enables debug logging
func DebugLogging() {
	log.SetLevel(log.DebugLevel)
}

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
		data := append(b.data, make([][]byte, b.size)...)
		b.data = data

		names := append(b.names, make([]string, b.size)...)
		b.names = names
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
			data := [][]byte{packets[n], buf[i]}
			packets[n] = bytes.Join(data, []byte("\n"))
		} else {
			packets[n] = buf[i]
		}
	}

	log.Debugf("%d unique names in buffer", len(packets))

	for n := range packets {
		log.Debugf("Sending data for name: %s (%d bytes)", n, len(packets[n]))
		thisErr := u.Send(packets[n], n)
		if thisErr != nil {
			log.Debugf("Send error: %s", err)
			err = thisErr
		}
	}
	if err == nil {
		// If all uploads suceeded, clear transmitted portion of buffer
		b.Lock()
		data := make([][]byte, len(b.data)-len(buf))
		copy(data, b.data[len(buf):])
		b.data = data

		names := make([]string, len(b.names)-len(buf))
		copy(names, b.names[len(buf):])
		b.names = names

		b.ref = b.ref - len(buf)
		b.Unlock()
	}
	return
}
