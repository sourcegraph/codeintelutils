package codeintelutils

import (
	"compress/gzip"
	"io"
	"os"

	"github.com/hashicorp/go-multierror"
)

// PartFilenameFunc constructs the name of a part file from its part index.
type PartFilenameFunc func(index int) string

// StitchFiles combines multiple compressed file parts into a single file. Each part on disk be concatenated
// into a single file. The content of each part is decompressed and written to the new file sequentially.
// On success, the part files are removed.
func StitchFiles(filename string, makePartFilename PartFilenameFunc, compress bool) error {
	targetFile, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := targetFile.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	var writer io.WriteCloser = targetFile
	if compress {
		writer = gzip.NewWriter(writer)
		defer func() {
			if closeErr := writer.Close(); closeErr != nil {
				err = multierror.Append(err, closeErr)
			}
		}()
	}

	index := 0
	for {
		exists, reader, err := openPart(makePartFilename(index))
		if err != nil {
			return err
		}
		if !exists {
			break
		}
		defer reader.Close()

		if _, err := io.Copy(writer, reader); err != nil {
			return err
		}

		index++
	}

	for i := index - 1; i >= 0; i-- {
		_ = os.Remove(makePartFilename(i))
	}

	return nil
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
