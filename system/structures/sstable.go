package structures

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
)

type SSTable struct {
	generalFilename string
	dataFilename    string
	indexFilename   string
	summaryFilename string
	filterFilename  string
}

func NewSSTable(data MemTable, filename string) (table *SSTable) {
	baseFilename := "system/data/sstable/usertable-data-ic-" + filename + "-lev1-"
	table = &SSTable{
		generalFilename: baseFilename,
		dataFilename:    baseFilename + "Data.db",
		indexFilename:   baseFilename + "Index.db",
		summaryFilename: baseFilename + "Summary.db",
		filterFilename:  baseFilename + "Filter.gob",
	}

	bloomFilter := CreateBF(data.Size(), 2)
	keys := make([]string, 0)
	offsets := make([]uint, 0)
	values := make([][]byte, 0)
	currentOffset := uint(0)

	file, err := os.Create(table.dataFilename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	// Write file length
	fileLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(fileLenBytes, uint64(data.Size()))
	bytesWritten, err := writer.Write(fileLenBytes)
	currentOffset += uint(bytesWritten)
	if err != nil {
		log.Fatal(err)
	}

	err = writer.Flush()
	if err != nil {
		return
	}

	// Iterate over data and write to SSTable file
	for node := data.data.head.Next[0]; node != nil; node = node.Next[0] {
		key, value := node.Key, node.Value
		keys = append(keys, key)
		offsets = append(offsets, currentOffset)
		values = append(values, value)

		bloomFilter.Add(*node)

		// Write Checksum
		crc := CRC32(value)
		crcBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(crcBytes, crc)
		bytesWritten, err := writer.Write(crcBytes)
		currentOffset += uint(bytesWritten)
		if err != nil {
			return
		}

		// Write Timestamp
		timestamp := node.Timestamp
		timestampBytes := make([]byte, 19)
		copy(timestampBytes, timestamp)
		bytesWritten, err = writer.Write(timestampBytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		// Write Tombstone
		tombstone := node.Tombstone
		tombstoneInt := uint8(0)
		if tombstone {
			tombstoneInt = 1
		}

		err = writer.WriteByte(tombstoneInt)
		currentOffset += 1
		if err != nil {
			return
		}

		// Write Key
		keyBytes := []byte(key)
		keyLen := uint64(len(keyBytes))
		writeVarUint(writer, keyLen)
		currentOffset += writeBytes(writer, keyBytes)

		// Write Value
		valueLen := uint64(len(value))
		writeVarUint(writer, valueLen)
		currentOffset += writeBytes(writer, value)

		err = writer.Flush()
		if err != nil {
			return
		}
	}

}
