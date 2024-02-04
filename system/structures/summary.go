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

	fileLengthBytes := make([]byte, 8)
	_, err = reader.Read(fileLengthBytes)
	if err != nil {
		panic(err)
	}
	fileLength := binary.LittleEndian.Uint64(fileLengthBytes)

	startKeyLengthBytes := make([]byte, 8)
	_, err = reader.Read(startKeyLengthBytes)
	if err != nil {
		panic(err)
	}
	startKeyLength := binary.LittleEndian.Uint64(startKeyLengthBytes)

	startKeyBytes := make([]byte, startKeyLength)
	_, err = reader.Read(startKeyBytes)
	if err != nil {
		panic(err)
	}
	firstKey := string(startKeyBytes[:])

	if targetKey < firstKey {
		return false, 0
	}

	endKeyLengthBytes := make([]byte, 8)
	_, err = reader.Read(endKeyLengthBytes)
	if err != nil {
		panic(err)
	}
	endKeyLength := binary.LittleEndian.Uint64(endKeyLengthBytes)

	endKeyBytes := make([]byte, endKeyLength)
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
	for i = 0; i < fileLength-2; i++ {
		keyBytes := make([]byte, 8)
		_, err = reader.Read(keyBytes)
		if err != nil {
			panic(err)
		}
		nodeKeyLength := binary.LittleEndian.Uint64(keyBytes)

		nodeKeyBytes := make([]byte, nodeKeyLength)
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

	fileLength := uint64(len(keys))
	fileLengthBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(fileLengthBytes, fileLength)
	_, err = writer.Write(fileLengthBytes)
	if err != nil {
		log.Fatal(err)
	}

	for i := range keys {
		key := keys[i]
		offset := offsets[i]

		keyBytes := []byte(key)

		keyLength := uint64(len(keyBytes))
		keyLengthBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(keyLengthBytes, keyLength)
		_, err := writer.Write(keyLengthBytes)
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
