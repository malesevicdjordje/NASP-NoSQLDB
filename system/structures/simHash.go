package structures

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"
)

func generateWordsStop() map[string]bool {
	wordsStop := []string{"A", "About", "Actually", "Almost", "Also", "Although", "Always", "Am", "An", "And", "Any", "Are",
		"As", "At", "Be", "Became", "Become", "But", "By", "Can", "Could", "Did", "Do", "Does", "Each", "Either", "Else", "For",
		"From", "Had", "Has", "Have", "Hence", "How", "I", "If", "In", "IS", "IT", "ITS", "JUST", "MAY", "MAYBE", "Me", "Might",
		"Mine", "Must", "My", "Neither", "Nor", "Not", "Of", "Oh", "Ok", "When", "Where", "Whereas", "Wherever", "Whenever",
		"Whether", "Which", "While", "Who", "Whom", "Whoever", "Whose", "Why", "Will", "With", "Within", "Without",
		"Would", "Yes", "Yet", "You", "Your"}
	mapWordsStop := make(map[string]bool)
	for _, word := range wordsStop {
		mapWordsStop[strings.ToUpper(word)] = true
	}
	return mapWordsStop
}

type SimHash struct {
	mapWordsStop map[string]bool
}

func CreateSimHash() SimHash {
	mapWordsStop := generateWordsStop()
	return SimHash{mapWordsStop}
}

type Text struct {
	fingerprint []int
}

func CalculateNumOfWords(filepath string, simHash SimHash) map[string]int {
	mapWordsOccurencies := make(map[string]int)
	f, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		if simHash.mapWordsStop[scanner.Text()] == false {
			mapWordsOccurencies[strings.ToUpper(scanner.Text())] += 1
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return mapWordsOccurencies
}

func HashWords(mapWordsOccurencies map[string]int) map[int]string {

	hash := make(map[int]string)
	for word, _ := range mapWordsOccurencies {
		hash[mapWordsOccurencies[word]] = ToBinary(GetMD5Hash(word))
	}
	return hash
}

func SumHashesWords(hashs map[int]string) []int {

	fingerprint := make([]int, 256, 256)
	for occurency, hash := range hashs {
		for i := 0; i < len(hash); i++ {
			if hash[i] == '1' {
				fingerprint[i] += occurency
			} else {
				fingerprint[i] -= occurency
			}
		}
	}
	for i, value := range fingerprint {
		if value > 0 {
			fingerprint[i] = 1
		} else {
			fingerprint[i] = 0
		}
	}
	return fingerprint
}

// Funkcija koja pretvara text u heksadecimalni zapis izrazen u stringu
func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

// Funkcija koja pretvara heksadecimalni zapis izrazen u stringu u binarni
// zapis izrazen u stringu
func ToBinary(s string) string {
	res := ""
	for _, c := range s {
		res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}
