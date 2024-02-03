package structures

import (
	"hash"
	"math"
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
