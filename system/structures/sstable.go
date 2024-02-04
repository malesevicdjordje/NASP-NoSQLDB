package structures

import (
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

}
