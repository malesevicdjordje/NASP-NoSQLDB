package structures

import (
	"bufio"
	"encoding/binary"
	"log"
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
}
