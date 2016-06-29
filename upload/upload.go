package upload

import (
	"bytes"
	"compress/gzip"
	"errors"
	"net/http"
)

// GzipThreshold sets the threshold size over which messages
// are compressed when sent.
var GzipThreshold = 2 << 16

// Uploader is a reusable object to upload data to a single
// Sumologic HTTP collector.
type Uploader struct {
	url       string
	multiline bool
}

// NewUploader creates a new uploader.
func NewUploader(url string) *Uploader {
	u := new(Uploader)
	u.url = url
	return u
}

// Send sends a message to a Sumologic HTTP collector.  It will
// automatically compress messages larger than GzipThreshold.  Optionally,
// a name will be specified, if so this will be added as metadata.
func (u *Uploader) Send(input []byte, name ...string) (err error) {
	// nil input is a noop
	if input == nil {
		return
	}

	client := new(http.Client)
	buf := new(bytes.Buffer)

	req, err := http.NewRequest("POST", u.url, buf)
	if err != nil {
		return
	}

	if len(input) > GzipThreshold {
		w := gzip.NewWriter(buf)
		n, err := w.Write(input)
		if err != nil {
			return err
		}
		if n != len(input) {
			return errors.New("Error compressing data")
		}
		err = w.Close()
		if err != nil {
			return err
		}

		req.Header.Set("Content-Encoding", "gzip")
	} else {
		n, err := buf.Write(input)
		if err != nil {
			return err
		}
		if n != len(input) {
			return errors.New("Error compressing data")
		}
	}

	if name != nil {
		req.Header.Set("X-Sumo-Name", name[0])
	}

	resp, err := client.Do(req)
	if err != nil {
		return
	}

	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}

	return nil
}
