package structures

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"time"
)

type BloomFilter struct {
	Set           []byte        // set sa bitovima
	hashFunctions []hash.Hash32 // slice hash funkcija
	K             uint          // broj hash funkcija
	M             uint          // duzina (velicina) bloom filtera
	P             float64       // verovatnoca false-positive
	TimeSeconds   uint          // vreme u sekundama od 1.1.1970
}

func GenerateHashFunctions(k uint) ([]hash.Hash32, uint) {
	var hashFuncs []hash.Hash32
	seconds := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		hashFuncs = append(hashFuncs, murmur3.New32WithSeed(uint32(seconds+1)))
	}
	return hashFuncs, seconds
}

// Generise broj hash funkcija
func EvaluateK(numOfElements int, sizeOfFilter uint) uint {
	return uint(math.Ceil((float64(sizeOfFilter) / float64(numOfElements)) * math.Log(2)))
}

// Generise velicinu bloom filtera
func EvaluateM(numOfElements int, falsePositive float64) uint {
	return uint(math.Ceil(float64(numOfElements) * math.Abs(math.Log(falsePositive)) / math.Pow(math.Log(2), float64(2))))
}

func CreateBF(numOfElements uint, falsePositive float64) *BloomFilter {
	sizeOfFilter := EvaluateM(int(numOfElements), falsePositive)
	numOfHashFunctions := EvaluateK(int(numOfElements), sizeOfFilter)
	hashFs, seconds := GenerateHashFunctions(numOfHashFunctions)
	filter := BloomFilter{Set: make([]byte, sizeOfFilter), hashFunctions: hashFs, K: numOfHashFunctions, M: sizeOfFilter, P: falsePositive, TimeSeconds: seconds}
	return &filter
}
