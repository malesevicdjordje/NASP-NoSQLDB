package structures

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
)

// FindSummaryByKey searches for a summary in a file by a given key,
// returning a boolean indicating whether the key is found and the associated offset.
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

	found = true
	var i uint64
	for i = 0; i < fileLen-2; i++ {
		keyBytes := make([]byte, 8)
		_, err = reader.Read(keyBytes)
		if err != nil {
			panic(err)
		}
		nodeKeyLen := binary.LittleEndian.Uint64(keyBytes)

		nodeKeyBytes := make([]byte, nodeKeyLen)
		_, err = reader.Read(nodeKeyBytes)
		if err != nil {
			panic(err)
		}
		nodeKey := string(nodeKeyBytes[:])

		if nodeKey <= targetKey {
			offsetBytes := make([]byte, 8)
			_, err = reader.Read(offsetBytes)
			if err != nil {
				panic(err)
			}
			offset = int64(binary.LittleEndian.Uint64(offsetBytes))
		} else {
			break
		}
	}
	return
}

// WriteSummaryToFile creates a summary file with provided keys and corresponding offsets.
func WriteSummaryToFile(keys []string, offsets []uint, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	fileLen := uint64(len(keys))
	fileLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(fileLenBytes, fileLen)
	_, err = writer.Write(fileLenBytes)
	if err != nil {
		log.Fatal(err)
	}

	for i := range keys {
		key := keys[i]
		offset := offsets[i]

		keyBytes := []byte(key)

		keyLen := uint64(len(keyBytes))
		keyLenBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(keyLenBytes, keyLen)
		_, err := writer.Write(keyLenBytes)
		if err != nil {
			log.Fatal(err)
		}

		_, err = writer.Write(keyBytes)
		if err != nil {
			log.Fatal(err)
		}

		if i >= 2 {
			offsetBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(offsetBytes, uint64(offset))
			_, err = writer.Write(offsetBytes)
			if err != nil {
				log.Fatal(err)
			}
		}
		err = writer.Flush()
		if err != nil {
			return
		}
	}
}
