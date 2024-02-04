package structures

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"os"
	"strings"
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

	index := NewSimpleIndex(keys, offsets, table.indexFilename)
	indexKeys, indexOffsets := index.WriteToFile()
	WriteSummaryToFile(indexKeys, indexOffsets, table.summaryFilename)
	writeBF(table.filterFilename, bloomFilter)
	BuildMerkleTree(values, strings.ReplaceAll(table.dataFilename, "kv-system/data/sstable/", ""))
	table.WriteTableOfContents()

	return
}

func (st *SSTable) WriteTableOfContents() {
	filename := st.generalFilename + "TOC.txt"
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	writeLine(writer, st.dataFilename)
	writeLine(writer, st.indexFilename)
	writeLine(writer, st.summaryFilename)
	writeLine(writer, st.filterFilename)

	err = writer.Flush()
	if err != nil {
		return
	}
}

func (st *SSTable) FindRecord(key string, offset int64) (found bool, value []byte, timestamp string) {
	found = false
	timestamp = ""

	file, err := os.Open(st.dataFilename)
	if err != nil {
		panic(err)
	}

	reader := bufio.NewReader(file)
	fileLenBytes := make([]byte, 8)
	_, err = reader.Read(fileLenBytes)
	if err != nil {
		panic(err)
	}
	fileLen := binary.LittleEndian.Uint64(fileLenBytes)

	_, err = file.Seek(offset, 0)
	if err != nil {
		return false, nil, ""
	}
	reader = bufio.NewReader(file)

	for i := uint64(0); i < fileLen; i++ {
		var deleted bool

		// Read CRC
		crcBytes := readBytes(reader, 4)
		crcValue := binary.LittleEndian.Uint32(crcBytes)

		// Read Timestamp
		timestampBytes := readBytes(reader, 19)
		timestamp = string(timestampBytes[:])

		// Read Tombstone
		tombstoneByte, err := reader.ReadByte()
		if err != nil {
			panic(err)
		}
		deleted = tombstoneByte == 1

		// Read Key
		keyLen := readVarUint(reader)
		keyBytes := readBytes(reader, int(keyLen))
		nodeKey := string(keyBytes[:])

		if nodeKey == key {
			found = true
		}

		// Read Value
		valueLen := readVarUint(reader)
		value = readBytes(reader, int(valueLen))

		if found && !deleted && CRC32(value) == crcValue {
			break
		} else if found && deleted {
			return false, nil, ""
		}
	}

	file.Close()
	return found, value, timestamp
}

func readSSTable(filename, level string) (table *SSTable) {
	filename = "system/data/sstable/usertable-data-ic-" + filename + "-lev" + level + "-TOC.txt"

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	dataFilename := readLine(reader)
	indexFilename := readLine(reader)
	summaryFilename := readLine(reader)
	filterFilename := readLine(reader)
	generalFilename := strings.ReplaceAll(dataFilename, "Data.db", "")

	table = &SSTable{
		generalFilename: generalFilename,
		dataFilename:    dataFilename,
		indexFilename:   indexFilename,
		summaryFilename: summaryFilename,
		filterFilename:  filterFilename,
	}

	return
}

func (st *SSTable) QueryRecord(key string) (found bool, value []byte, timestamp string) {
	found = false
	value = nil
	bf := readBF(st.filterFilename)
	found = bf.Search(key)
	if found {
		found, offset := FindSummaryByKey(key, st.summaryFilename)
		if found {
			found, offset = SearchIndex(key, offset, st.indexFilename)
			if found {
				found, value, timestamp = st.FindRecord(key, offset)
				if found {
					return true, value, timestamp
				}
			}
		}
	}
	return false, nil, ""
}

// Helper functions

func writeVarUint(writer *bufio.Writer, value uint64) (written uint) {
	bytes := make([]byte, 0, 10)
	for value >= 0x80 {
		bytes = append(bytes, byte(value|0x80))
		value >>= 7
	}
	bytes = append(bytes, byte(value))
	written, _ = writer.Write(bytes)
	return
}

func writeBytes(writer *bufio.Writer, data []byte) (written uint) {
	written, _ = writer.Write(data)
	return
}

func readVarUint(reader *bufio.Reader) (value uint64) {
	shift := uint(0)
	for {
		b, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		value |= uint64(b&0x7F) << shift
		shift += 7
		if b&0x80 == 0 {
			break
		}
	}
	return
}

func readBytes(reader *bufio.Reader, length int) (data []byte) {
	data = make([]byte, length)
	_, err := io.ReadFull(reader, data)
	if err != nil {
		panic(err)
	}
	return
}

func writeLine(writer *bufio.Writer, line string) {
	_, err := writer.WriteString(line + "\n")
	if err != nil {
		panic(err)
	}
}

func readLine(reader *bufio.Reader) (line string) {
	line, err := reader.ReadString('\n')
	if err != nil {
		panic(err)
	}
	return strings.TrimSpace(line)
}
