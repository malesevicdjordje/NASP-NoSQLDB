package structures

import "os"

func FindSummaryByKey(targetKey, filename string) (found bool, offset int64) {
	found = false
	offset = int64(8)

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

}
