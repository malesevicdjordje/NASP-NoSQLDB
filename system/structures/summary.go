package structures

import (
	"bufio"
	"encoding/binary"
	"os"
)

func FindSummaryByKey(targetKey, filename string) (found bool, offset int64) {
	found = false
	offset = int64(8)

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	fileLenBytes := make([]byte, 8)
	_, err = reader.Read(fileLenBytes)
	if err != nil {
		panic(err)
	}
	fileLen := binary.LittleEndian.Uint64(fileLenBytes)

	startKeyLenBytes := make([]byte, 8)
	_, err = reader.Read(startKeyLenBytes)
	if err != nil {
		panic(err)
	}
	startKeyLen := binary.LittleEndian.Uint64(startKeyLenBytes)

	startKeyBytes := make([]byte, startKeyLen)
	_, err = reader.Read(startKeyBytes)
	if err != nil {
		panic(err)
	}
	firstKey := string(startKeyBytes[:])

	if targetKey < firstKey {
		return false, 0
	}

	endKeyLenBytes := make([]byte, 8)
	_, err = reader.Read(endKeyLenBytes)
	if err != nil {
		panic(err)
	}
	endKeyLen := binary.LittleEndian.Uint64(endKeyLenBytes)

	endKeyBytes := make([]byte, endKeyLen)
	_, err = reader.Read(endKeyBytes)
	if err != nil {
		panic(err)
	}
	lastKey := string(endKeyBytes[:])

	if targetKey > lastKey {
		return false, 0
	}

}
