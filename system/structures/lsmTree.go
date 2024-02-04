package structures

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
