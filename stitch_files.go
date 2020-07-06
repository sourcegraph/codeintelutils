package codeintelutils

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-multierror"
)

// PartFilenameFunc constructs the name of a part file from its part index.
type PartFilenameFunc func(index int) string

// StitchFiles combines multiple compressed file parts into a single file. Each part on disk be concatenated
// into a single file. The content of each part is decompressed and written to the new file sequentially.
// On success, the part files are removed.
func StitchFiles(filename string, makePartFilename PartFilenameFunc, compress bool) error {
	if err := os.MkdirAll(filepath.Dir(filename), os.ModePerm); err != nil {
		return err
	}

	targetFile, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := targetFile.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	r, err := StitchFilesReader(makePartFilename)
	if err != nil {
		return err
	}

	if compress {
		r = Gzip(r)
	}

	_, err = io.Copy(targetFile, r)
	return err
}

// StitchFilesReader combines multiple compressed file parts into a single reader. Each part on disk is
// concatenated into a single file. The content of each part is decompressed and written to returned
// reader sequentially. On success, the part files are removed.
func StitchFilesReader(makePartFilename PartFilenameFunc) (io.Reader, error) {
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		index := 0
		for {
			ok, err := writePart(pw, makePartFilename(index))
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			if !ok {
				break
			}

			index++
		}

		for i := index - 1; i >= 0; i-- {
			_ = os.Remove(makePartFilename(i))
		}
	}()

	return pr, nil
}

// WritePart opens the given filename and writes its content to the given writer.
// Returns a boolean flag indicating whether or not a file was opened for reading.
func writePart(w io.Writer, filename string) (bool, error) {
	exists, reader, err := openPart(filename)
	if err != nil || !exists {
		return false, err
	}
	defer reader.Close()

	_, err = io.Copy(w, reader)
	return true, err
}

// openPart opens a gzip reader for a upload part file as well as a boolean flag
// indicating if the file exists.
func openPart(filename string) (bool, io.ReadCloser, error) {
	f, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil, nil
		}

		return false, nil, err
	}

	reader, err := gzip.NewReader(f)
	if err != nil {
		return false, nil, err
	}

	return true, &partReader{reader, f}, nil
}

// partReader bundles a gzip reader with its underlying reader. This overrides the
// Close method on the gzip reader so that it also closes the underlying reader.
type partReader struct {
	*gzip.Reader
	rc io.ReadCloser
}

func (r *partReader) Close() error {
	for _, err := range []error{r.Reader.Close(), r.rc.Close()} {
		if err != nil {
			return err
		}
	}

	return nil
}
