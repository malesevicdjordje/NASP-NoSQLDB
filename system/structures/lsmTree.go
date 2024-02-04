package structures

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

type LSMTree struct {
	maxLevel int
	maxSize  int
}

func NewLSMTree(maxLevels, maxSize int) *LSMTree {
	return &LSMTree{
		maxLevel: maxLevels,
		maxSize:  maxSize,
	}
}

func (tree LSMTree) IsCompactionNeeded(directory string, level int) (bool, []string, []string, []string, []string, []string) {
	dataFiles, indexFiles, summaryFiles, tocFiles, filterFiles := FindFiles(directory, level)
	return len(indexFiles) == tree.maxSize, dataFiles, indexFiles, summaryFiles, tocFiles, filterFiles
}

func (tree LSMTree) PerformCompaction(directory string, level int) {
	if level >= tree.maxLevel {
		return // No compaction after reaching the last level.
	}

	compactionNeeded, dataFiles, indexFiles, summaryFiles, tocFiles, filterFiles := tree.IsCompactionNeeded(directory, level)
	if !compactionNeeded {
		return
	}

	//compaction is needed
	_, indexFilesNextLevel, _, _, _ := FindFiles(directory, level+1)

	var numFile int
	if len(indexFilesNextLevel) == 0 {
		numFile = 1
	} else {
		numFile = len(indexFilesNextLevel) + 1
	}

	for i := 0; i < tree.maxSize; i += 2 {
		firstDataFile, firstIndexFile, firstSummaryFile, firstTocFile, firstFilterFile :=
			dataFiles[i], indexFiles[i], summaryFiles[i], tocFiles[i], filterFiles[i]

		secondDataFile, secondIndexFile, secondSummaryFile, secondTocFile, secondFilterFile :=
			dataFiles[i+1], indexFiles[i+1], summaryFiles[i+1], tocFiles[i+1], filterFiles[i+1]

		MergeTables(directory, numFile, level+1, firstDataFile, firstIndexFile, firstSummaryFile, firstTocFile, firstFilterFile,
			secondDataFile, secondIndexFile, secondSummaryFile, secondTocFile, secondFilterFile)

		numFile++
	}

	tree.PerformCompaction(directory, level+1)
}

func MergeTables(directory string, numFile, level int, firstData, firstIndex, firstSummary, firstToc, firstFilter,
	secondData, secondIndex, secondSummary, secondToc, secondFilter string) {

	strLevel := strconv.Itoa(level + 1)
	generalFilename := directory + "usertable-data-ic-" + strconv.Itoa(numFile) + "-lev" + strLevel + "-"
	mergedTable := &SSTable{generalFilename, generalFilename + "Data.db",
		generalFilename + "Index.db", generalFilename + "Summary.db",
		generalFilename + "Filter.gob"}

	newData, err := os.Create(mergedTable.generalFilename + "Data.db")
	if err != nil {
		log.Fatal(err)
	}

	currentOffset := uint(0)
	currentOffset1 := uint(0)
	currentOffset2 := uint(0)

	writer := bufio.NewWriter(newData)
	bytesLen := make([]byte, 8)

	bytesWritten, err := writer.Write(bytesLen)
	currentOffset += uint(bytesWritten)
	if err != nil {
		log.Fatal(err)
	}

	firstDataFile, err := os.Open(directory + firstData)
	if err != nil {
		panic(err)
	}

	secondDataFile, err := os.Open(directory + secondData)
	if err != nil {
		panic(err)
	}

	reader1 := bufio.NewReader(firstDataFile)
	bytes := make([]byte, 8)
	_, err = reader1.Read(bytes)
	if err != nil {
		panic(err)
	}
	fileLen1 := binary.LittleEndian.Uint64(bytes)
	currentOffset1 += 8

	// file length drugog data fajla
	reader2 := bufio.NewReader(secondDataFile)
	bytes = make([]byte, 8)
	_, err = reader2.Read(bytes)
	if err != nil {
		panic(err)
	}
	fileLen2 := binary.LittleEndian.Uint64(bytes)
	currentOffset2 += 8

	fileLen := readAndWriteData(currentOffset, currentOffset1, currentOffset2, newData, firstDataFile, secondDataFile,
		fileLen1, fileLen2, mergedTable, level)

	FileSize(mergedTable.dataFilename, fileLen)

	_ = newData.Close()
	_ = firstDataFile.Close()
	_ = secondDataFile.Close()

	_ = os.Remove(directory + firstData)
	_ = os.Remove(directory + firstIndex)
	_ = os.Remove(directory + firstSummary)
	_ = os.Remove(directory + firstToc)
	_ = os.Remove(directory + firstFilter)
	_ = os.Remove(directory + secondData)
	_ = os.Remove(directory + secondIndex)
	_ = os.Remove(directory + secondSummary)
	_ = os.Remove(directory + secondToc)
	_ = os.Remove(directory + secondFilter)
}

func readAndWriteData(currentOffset, currentOffset1, currentOffset2 uint, newData, firstDataFile, secondDataFile *os.File,
	fileLen1, fileLen2 uint64, table *SSTable, level int) uint64 {

	filter := CreateBF(uint(fileLen1+fileLen2), 2)
	keys := make([]string, 0)
	offsets := make([]uint, 0)
	values := make([][]byte, 0)

	crc1, timestamp1, tombstone1, keyLen1, valueLen1, key1, value1, currentOffset1 :=
		readData(firstDataFile, currentOffset1)

	crc2, timestamp2, tombstone2, keyLen2, valueLen2, key2, value2, currentOffset2 :=
		readData(secondDataFile, currentOffset2)

	first, second := uint64(0), uint64(0)
	for {
		if fileLen1 == first || fileLen2 == second {
			break
		}

		if key1 == key2 {
			if timestamp1 > timestamp2 {
				if tombstone1 == 0 {
					offsets = append(offsets, currentOffset)
					currentOffset = writeData(newData, currentOffset, crc1, timestamp1, tombstone1,
						keyLen1, valueLen1, key1, value1)
					filter.Add(Element{0, timestamp1, false, key1, nil, nil})
					keys = append(keys, key1)
					values = append(values, []byte(value1))
				}
			} else {
				if tombstone2 == 0 {
					offsets = append(offsets, currentOffset)
					currentOffset = writeData(newData, currentOffset, crc2, timestamp2, tombstone2,
						keyLen2, valueLen2, key2, value2)
					filter.Add(Element{0, timestamp2, false, key2, nil, nil})
					keys = append(keys, key2)
					values = append(values, []byte(value2))
				}
			}

			if fileLen1-1 > first {
				crc1, timestamp1, tombstone1, keyLen1, valueLen1, key1, value1, currentOffset1 =
					readData(firstDataFile, currentOffset1)
			}
			first++

			if fileLen2-1 > second {
				crc2, timestamp2, tombstone2, keyLen2, valueLen2, key2, value2, currentOffset2 =
					readData(secondDataFile, currentOffset2)
			}
			second++

		} else if key1 < key2 {
			if tombstone1 == 0 {
				offsets = append(offsets, currentOffset)
				currentOffset = writeData(newData, currentOffset, crc1, timestamp1, tombstone1,
					keyLen1, valueLen1, key1, value1)
				filter.Add(Element{0, timestamp1, false, key1, nil, nil})
				keys = append(keys, key1)
				values = append(values, []byte(value1))
			}

			if fileLen1-1 > first {
				crc1, timestamp1, tombstone1, keyLen1, valueLen1, key1, value1, currentOffset1 =
					readData(firstDataFile, currentOffset1)
			}
			first++

		} else {
			if tombstone2 == 0 {
				offsets = append(offsets, currentOffset)
				currentOffset = writeData(newData, currentOffset, crc2, timestamp2, tombstone2,
					keyLen2, valueLen2, key2, value2)
				filter.Add(Element{0, timestamp2, false, key2, nil, nil})
				keys = append(keys, key2)
				values = append(values, []byte(value2))
			}

			if fileLen2-1 > second {
				crc2, timestamp2, tombstone2, keyLen2, valueLen2, key2, value2, currentOffset2 =
					readData(secondDataFile, currentOffset2)
			}
			second++

		}
	}

	if fileLen1 == first && fileLen2 != second {
		for fileLen2 != second {
			if tombstone2 == 0 {
				offsets = append(offsets, currentOffset)
				currentOffset = writeData(newData, currentOffset, crc2, timestamp2, tombstone2,
					keyLen2, valueLen2, key2, value2)
				filter.Add(Element{0, timestamp2, false, key2, nil, nil})
				keys = append(keys, key2)
				values = append(values, []byte(value2))
			}

			if fileLen2-1 > second {
				crc2, timestamp2, tombstone2, keyLen2, valueLen2, key2, value2, currentOffset2 =
					readData(secondDataFile, currentOffset2)
			}
			second++

		}
	} else if fileLen2 == second && fileLen1 != first {
		for fileLen1 > first {
			if tombstone1 == 0 {
				offsets = append(offsets, currentOffset)
				currentOffset = writeData(newData, currentOffset, crc1, timestamp1, tombstone1,
					keyLen1, valueLen1, key1, value1)
				filter.Add(Element{0, timestamp1, false, key1, nil, nil})
				keys = append(keys, key1)
				values = append(values, []byte(value1))
			}

			if fileLen1-1 != first {
				crc1, timestamp1, tombstone1, keyLen1, valueLen1, key1, value1, currentOffset1 =
					readData(firstDataFile, currentOffset1)
			}
			first++
		}
	}

	index := NewSimpleIndex(keys, offsets, table.indexFilename)
	keysIndex, offsetsIndex := index.WriteToFile()
	WriteSummaryToFile(keysIndex, offsetsIndex, table.summaryFilename)
	table.WriteTableOfContents()
	writeBF(table.filterFilename, filter)
	CreateMerkle(level, newData.Name(), values)

	return uint64(len(keys))
}

// writeData writes a key-value pair to a file, updating the file offset.
func writeData(file *os.File, currentOffset uint, crcBytes []byte, timestamp string, tombstone byte,
	keyLen, valueLen uint64, key, value string) uint {

	if tombstone == 1 {
		return currentOffset
	}

	file.Seek(int64(currentOffset), 0)
	writer := bufio.NewWriter(file)

	bytesWritten, err := writer.Write(crcBytes)
	currentOffset += uint(bytesWritten)
	if err != nil {
		log.Fatal(err)
	}

	// Timestamp
	timestampBytes := make([]byte, 19)
	copy(timestampBytes, timestamp)
	bytesWritten, err = writer.Write(timestampBytes)
	if err != nil {
		log.Fatal(err)
	}
	currentOffset += uint(bytesWritten)

	// Tombstone
	tombstoneInt := tombstone
	err = writer.WriteByte(tombstoneInt)
	currentOffset += 1
	if err != nil {
		log.Fatal(err)
	}

	// keyLen
	keyLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyLenBytes, keyLen)
	bytesWritten, err = writer.Write(keyLenBytes)
	if err != nil {
		log.Fatal(err)
	}
	currentOffset += uint(bytesWritten)

	// valueLen
	valueLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueLenBytes, valueLen)
	bytesWritten, err = writer.Write(valueLenBytes)
	if err != nil {
		log.Fatal(err)
	}
	currentOffset += uint(bytesWritten)

	// Key
	keyBytes := []byte(key)
	bytesWritten, err = writer.Write(keyBytes)
	if err != nil {
		log.Fatal(err)
	}
	currentOffset += uint(bytesWritten)

	// Value
	valueBytes := []byte(value)
	bytesWritten, err = writer.Write(valueBytes)
	if err != nil {
		log.Fatal(err)
	}
	currentOffset += uint(bytesWritten)

	err = writer.Flush()
	if err != nil {
		log.Fatal(err)
	}

	return currentOffset
}

// readData reads a key-value pair from a file, returning relevant information.
func readData(file *os.File, currentOffset uint) ([]byte, string, byte, uint64, uint64, string, string, uint) {
	file.Seek(int64(currentOffset), 0)
	reader := bufio.NewReader(file)

	// crc
	crcBytes := make([]byte, 4)
	_, err := reader.Read(crcBytes)
	if err != nil {
		panic(err)
	}
	currentOffset += 4

	// Timestamp
	timestampBytes := make([]byte, 19)
	_, err = reader.Read(timestampBytes)
	if err != nil {
		panic(err)
	}
	timestamp := string(timestampBytes[:])
	currentOffset += 19

	// Tombstone
	tombstone, err := reader.ReadByte()
	if err != nil {
		panic(err)
	}
	currentOffset += 1

	// keyLen
	keyLenBytes := make([]byte, 8)
	_, err = reader.Read(keyLenBytes)
	if err != nil {
		panic(err)
	}
	keyLen := binary.LittleEndian.Uint64(keyLenBytes)
	currentOffset += 8

	// valueLen
	valueLenBytes := make([]byte, 8)
	_, err = reader.Read(valueLenBytes)
	if err != nil {
		panic(err)
	}
	valueLen := binary.LittleEndian.Uint64(valueLenBytes)
	currentOffset += 8

	// Key
	keyBytes := make([]byte, keyLen)
	_, err = reader.Read(keyBytes)
	if err != nil {
		panic(err)
	}
	key := string(keyBytes)
	currentOffset += uint(keyLen)

	// Value
	valueBytes := make([]byte, valueLen)
	_, err = reader.Read(valueBytes)
	if err != nil {
		panic(err)
	}
	value := string(valueBytes)
	currentOffset += uint(valueLen)

	return crcBytes, timestamp, tombstone, keyLen, valueLen, key, value, currentOffset
}

func CreateMerkle(level int, newData string, values [][]byte) {
	files, _ := ioutil.ReadDir("./system/data/metadata/") // lista svih fajlova iz direktorijuma
	for _, f := range files {
		// brisemo sve metadata fajlove sa zadatog nivoa
		if strings.Contains(f.Name(), "lev"+strconv.Itoa(level)+"-Metadata.txt") {
			err := os.Remove("./system/data/metadata/" + f.Name())
			if err != nil {
				fmt.Println(err)
			}
		}
	}

	filename := strings.ReplaceAll(newData, "system/data/sstable/", "")
	BuildMerkleTree(values, filename)
}

func FindFiles(dir string, level int) ([]string, []string, []string, []string, []string) {
	substr := strconv.Itoa(level)

	files, _ := ioutil.ReadDir(dir) // lista svih fajlova iz direktorijuma

	var dataFiles []string
	var indexFiles []string
	var summaryFiles []string
	var tocFiles []string
	var filterFiles []string

	for _, f := range files {
		if strings.Contains(f.Name(), "lev"+substr+"-Data.db") {
			dataFiles = append(dataFiles, f.Name())
		}
		if strings.Contains(f.Name(), "lev"+substr+"-Index.db") {
			indexFiles = append(indexFiles, f.Name())
		}
		if strings.Contains(f.Name(), "lev"+substr+"-Summary.db") {
			summaryFiles = append(summaryFiles, f.Name())
		}
		if strings.Contains(f.Name(), "lev"+substr+"-TOC.txt") {
			tocFiles = append(tocFiles, f.Name())
		}
		if strings.Contains(f.Name(), "lev"+substr+"-Filter.gob") {
			filterFiles = append(filterFiles, f.Name())
		}
	}

	return dataFiles, indexFiles, summaryFiles, tocFiles, filterFiles
}

func FileSize(filename string, len uint64) {
	file, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	_, err = file.Seek(0, 0)

	writer := bufio.NewWriter(file)

	bytesLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesLen, len)
	_, err = writer.Write(bytesLen)

	if err != nil {
		log.Println(err)
	}

	err = writer.Flush()
	if err != nil {
		return
	}

	err = file.Close()
}
