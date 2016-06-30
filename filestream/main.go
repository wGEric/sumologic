package main // import "github.com/nutmegdevelopment/sumologic/filestream"

import (
	"bytes"
	"flag"
	"log"
	"sync"
	"time"

	"github.com/dleung/gotail"
	"github.com/nutmegdevelopment/sumologic/upload"
)

var (
	fileName string
	bTime    int
	url      string
	sendName string
	bSize    = 4096
	uploader *upload.Uploader
)

func init() {
	flag.StringVar(&fileName, "f", "", "File to stream")
	flag.StringVar(&url, "u", "http://localhost", "URL of sumologic collector")
	flag.StringVar(&sendName, "n", "", "Name to send to Sumologic")
	flag.IntVar(&bTime, "b", 5, "Maximum time to buffer messages before upload")
	flag.Parse()
}

func watchFile(b *Buffer) {
	t, err := gotail.NewTail(fileName, gotail.Config{Timeout: 10})
	if err != nil {
		log.Fatal(err)
	}
	for line := range t.Lines {
		b.Add([]byte(line))
	}
	log.Fatal("I/O Error")
}

func sender(in []byte) (err error) {
	if sendName == "" {
		err = uploader.Send(in)
	} else {
		err = uploader.Send(in, sendName)
	}
	return
}

// Buffer is a basic buffer structure.
type Buffer struct {
	sync.Mutex
	data [][]byte
	ref  int
}

// NewBuffer allocates a new buffer
func NewBuffer() *Buffer {
	b := new(Buffer)
	b.data = make([][]byte, bSize)
	return b
}

// Add appends data to the buffer
func (b *Buffer) Add(in []byte) {
	b.Lock()
	defer b.Unlock()
	if b.ref >= len(b.data) {
		b.data = append(b.data, make([][]byte, bSize)...)
	}
	b.data[b.ref] = in
	b.ref++
}

// Send transmits data in the buffer using a provided sendFunc.
// This is not safe to call concurrently.
func (b *Buffer) Send(sendFunc func([]byte) error) (err error) {
	b.Lock()
	buf := b.data[:b.ref]
	b.Unlock()

	data := bytes.Join(buf, []byte("\n"))
	err = sendFunc(data)
	if err == nil {
		// If upload suceeded, clear transmitted portion of buffer
		b.Lock()
		b.data = b.data[len(buf):]
		b.ref = b.ref - len(buf)
		b.Unlock()
	}

	return
}

func main() {
	b := NewBuffer()
	uploader = upload.NewUploader(url)

	go func() {
		for {
			time.Sleep(time.Second * time.Duration(bTime))
			b.Send(sender)
		}
	}()

	watchFile(b)
}
