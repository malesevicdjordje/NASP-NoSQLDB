package structures

import (
	"encoding/gob"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
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

func CreateBF(numOfElements uint, falsePositive float64) *BloomFilter {
	sizeOfFilter := EvaluateMForBloomF(int(numOfElements), falsePositive)
	numOfHashFunctions := EvaluateKForBloomF(int(numOfElements), sizeOfFilter)
	hashFs, seconds := GenerateHashFunctions(numOfHashFunctions)
	filter := BloomFilter{Set: make([]byte, sizeOfFilter), hashFunctions: hashFs, K: numOfHashFunctions, M: sizeOfFilter, P: falsePositive, TimeSeconds: seconds}
	return &filter
}

func GenerateHashFunctions(numOfHashFunctions uint) ([]hash.Hash32, uint) {
	var hashFuncs []hash.Hash32
	seconds := uint(time.Now().Unix())
	for i := uint(0); i < numOfHashFunctions; i++ {
		hashFuncs = append(hashFuncs, murmur3.New32WithSeed(uint32(seconds+1)))
	}
	return hashFuncs, seconds
}

// Generise broj hash funkcija
func EvaluateKForBloomF(numOfElements int, sizeOfFilter uint) uint {
	return uint(math.Ceil((float64(sizeOfFilter) / float64(numOfElements)) * math.Log(2)))
}

// Generise velicinu bloom filtera
func EvaluateMForBloomF(numOfElements int, falsePositive float64) uint {
	return uint(math.Ceil(float64(numOfElements) * math.Abs(math.Log(falsePositive)) / math.Pow(math.Log(2), float64(2))))
}

// Dodavanje elementa u bloom filter
func (bloomFilter *BloomFilter) Add(element Element) {
	for _, hashFunction := range bloomFilter.hashFunctions {
		i := HashTheKey(hashFunction, element.Key, bloomFilter.M)
		bloomFilter.Set[i] = 1
	}
}

// Pretraga elementa u bloom filteru
func (bloomFilter *BloomFilter) Search(key string) bool {
	for _, hashFunction := range bloomFilter.hashFunctions {
		i := HashTheKey(hashFunction, key, bloomFilter.M)
		if bloomFilter.Set[i] != 1 {
			return false
		}
	}
	return true
}

func HashTheKey(hashFunction hash.Hash32, key string, sizeOfFilter uint) uint32 {
	_, err := hashFunction.Write([]byte(key))
	if err != nil {
		panic(err)
	}
	index := hashFunction.Sum32() % uint32(sizeOfFilter)
	hashFunction.Reset()
	return index
}

func CopyHashFunctions(numOfHashFunctions uint, seconds uint) []hash.Hash32 {
	var hashFuncs []hash.Hash32
	for i := uint(0); i < numOfHashFunctions; i++ {
		hashFuncs = append(hashFuncs, murmur3.New32WithSeed(uint32(seconds+1)))
	}
	return hashFuncs
}

func readBF(filepath string) (filter *BloomFilter) {
	f, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	decoder := gob.NewDecoder(f)
	filter = new(BloomFilter)
	_, err = f.Seek(0, 0)
	if err != nil {
		return nil
	}

	for {
		err = decoder.Decode(filter)
		if err != nil {
			//fmt.Println(err)
			break
		}
		//fmt.Println(*filter)
	}
	filter.hashFunctions = CopyHashFunctions(filter.K, filter.TimeSeconds)
	return
}

func writeBF(filepath string, filter *BloomFilter) {
	f, err := os.Create(filepath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	encoder := gob.NewEncoder(f)
	err = encoder.Encode(filter)
	if err != nil {
		panic(err)
	}
}
