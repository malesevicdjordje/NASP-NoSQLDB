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
}
