package structures

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"time"
)

type CountMinSketch struct {
	Set           [][]int       // set sa bitovima
	hashFunctions []hash.Hash32 // slice hash funkcija
	K             uint          // broj hash funkcija
	M             uint          // duzina (velicina) Set-a
	Epsilon       float64       // preciznost
	Delta         float64       // tacnost (sigurnost)
	TimeSeconds   uint          // vreme u sekundama od 1.1.1970
}

func EvaluateMForCMS(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func EvaluateKForCMS(delta float64) uint {
	return uint(math.Ceil(math.Log(1 / delta)))
}

func GenerateHashFunctionsForCMS(numOfHashFunctions uint) ([]hash.Hash32, uint) {
	var hashFuncs []hash.Hash32
	seconds := uint(time.Now().Unix())
	for i := uint(0); i < numOfHashFunctions; i++ {
		hashFuncs = append(hashFuncs, murmur3.New32WithSeed(uint32(seconds+1)))
	}
	return hashFuncs, seconds
}

func HashTheKeyForCMS(hashFunction hash.Hash32, key string, sizeOfFilter uint) uint32 {
	_, err := hashFunction.Write([]byte(key))
	if err != nil {
		panic(err)
	}
	index := hashFunction.Sum32() % uint32(sizeOfFilter)
	hashFunction.Reset()
	return index
}
