package structures

import (
	"hash"
	"math"
)

type BloomFilter struct {
	Set         []byte        // set sa bitovima
	hashes      []hash.Hash32 // slice hash funkcija
	K           uint          // broj hash funkcija
	M           uint          // duzina (velicina) bloom filtera
	P           float64       // verovatnoca false-positive
	TimeSeconds uint          // vreme u sekundama od 1.1.1970
}

// Generise velicinu bloom filtera
func EvaluateM(numOfElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(numOfElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

// Generise broj hash funkcija
func EvaluateK(numOfElements int, sizeOfFilter uint) uint {
	return uint(math.Ceil((float64(sizeOfFilter) / float64(numOfElements)) * math.Log(2)))
}
