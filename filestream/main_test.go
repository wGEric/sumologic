package main // import "github.com/nutmegdevelopment/sumologic/filestream"

import (
	"errors"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBuffer(t *testing.T) {

	// Set a small buffer size to speed up testing
	bSize = 8

	buf := NewBuffer()

	for i := 0; i < 128; i++ {
		data := strconv.AppendInt([]byte{}, int64(i), 10)
		buf.Add(data)
	}

	err := buf.Send(func(in []byte) error {
		assert.NotEmpty(t, in)
		assert.Len(t, in, 401)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, buf.data, 0)

	for i := 0; i < 128; i++ {
		data := strconv.AppendInt([]byte{}, int64(i), 10)
		buf.Add(data)
	}

	err = buf.Send(func(in []byte) error {
		assert.NotEmpty(t, in)
		assert.Len(t, in, 401)
		return errors.New("error")
	})
	assert.Error(t, err)
	assert.Len(t, buf.data, 128)
}

func TestBufferRace(t *testing.T) {
	bSize = 8
	buf := NewBuffer()
	q1 := make(chan bool)
	q2 := make(chan bool)

	go func() {
		in := []byte("Some test data here")
		for {
			select {
			case <-q1:
				return
			default:
				buf.Add(in)
			}
		}
	}()

	go func() {
		in := []byte("Some test data here")
		for {
			select {
			case <-q2:
				return
			default:
				buf.Add(in)
			}
		}
	}()

	for i := 0; i < 10; i++ {

		buf.Send(func(in []byte) error {
			return nil
		})

		time.Sleep(50 * time.Millisecond)
	}

	q1 <- true
	q2 <- true
}

func TestWatchFile(t *testing.T) {
	b := NewBuffer()
	f, err := ioutil.TempFile("/tmp", "filestream-test")
	assert.NoError(t, err)

	go func() {
		f.WriteString("test1\n")
		f.WriteString("test2\n")
		f.WriteString("test3\n")
		f.Sync()
		f.Close()
		time.Sleep(3 * time.Second)
		os.Remove(f.Name())
	}()

	err = watchFile(b, f.Name())
	assert.NoError(t, err)

	assert.Equal(t, []byte("test1"), b.data[0])
	assert.Equal(t, []byte("test2"), b.data[1])
	assert.Equal(t, []byte("test3"), b.data[2])

	err = watchFile(b, f.Name())
	assert.Error(t, err)
}
