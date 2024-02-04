package structures

import (
	"bufio"
	"log"
	"os"
	"strconv"
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

	mergedTable := &SSTable{
		generalFilename: directory + "usertable-data-ic-" + strconv.Itoa(numFile) + "-lev" + strconv.Itoa(level) + "-",
		dataFilename:    "",
		indexFilename:   "",
		summaryFilename: "",
		filterFilename:  "",
	}

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
	fileLen1, currentOffset1 := readFileSize(reader1, currentOffset1)

	reader2 := bufio.NewReader(secondDataFile)
	fileLen2, currentOffset2 := readFileSize(reader2, currentOffset2)

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
