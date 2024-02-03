package structures

import (
	"bytes"
	"encoding/gob"
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

func CreateCountMinSketch(epsilon float64, delta float64) *CountMinSketch {
	k := EvaluateKForCMS(delta)
	m := EvaluateMForCMS(epsilon)
	hashFs, seconds := GenerateHashFunctionsForCMS(k)
	set := make([][]int, k, k)
	for i, _ := range set {
		set[i] = make([]int, m, m)
	}
	cms := CountMinSketch{set, hashFs, k, m, epsilon, delta, seconds}
	//fmt.Printf("Created Count Min Skatch with M = %d, K = %d\n", M, k)
	return &cms
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

func (countMinSketch *CountMinSketch) Add(key string) {
	for i, hashFunction := range countMinSketch.hashFunctions {
		j := HashTheKeyForCMS(hashFunction, key, countMinSketch.M)
		countMinSketch.Set[i][j] += 1
	}
	//fmt.Printf("Element %s added !\n", key)
}

func (countMinSketch *CountMinSketch) Search(key string) int {
	values := make([]int, countMinSketch.K, countMinSketch.K)
	for i, hashFunction := range countMinSketch.hashFunctions {
		j := HashTheKeyForCMS(hashFunction, key, countMinSketch.M)
		values[i] = countMinSketch.Set[i][j]
	}

	minValue := values[0]
	for _, value := range values {
		if value < minValue {
			minValue = value
		}
	}
	return minValue
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

func CopyHashFunctionsForCMS(numOfHashFunctions uint, seconds uint) []hash.Hash32 {
	var hashFuncs []hash.Hash32
	for i := uint(0); i < numOfHashFunctions; i++ {
		hashFuncs = append(hashFuncs, murmur3.New32WithSeed(uint32(seconds+1)))
	}
	return hashFuncs
}

func (countMinSketch *CountMinSketch) SerializeCMS() []byte {

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(countMinSketch)
	if err != nil {
		return nil
	}
	return buffer.Bytes()
}

func DeserializeCMS(data []byte) *CountMinSketch {

	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	countMinSketch := new(CountMinSketch)

	for {
		err := decoder.Decode(countMinSketch)
		if err != nil {
			break
		}
	}
	countMinSketch.hashFunctions = CopyHashFunctionsForCMS(countMinSketch.K, countMinSketch.TimeSeconds)
	return countMinSketch
}
