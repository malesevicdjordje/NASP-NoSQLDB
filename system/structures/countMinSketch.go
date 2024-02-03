package structures

import "hash"

type CountMinSketch struct {
	Set           [][]int       // set sa bitovima
	hashFunctions []hash.Hash32 // slice hash funkcija
	K             uint          // broj hash funkcija
	M             uint          // duzina (velicina) Set-a
	E             float64       // preciznost
	D             float64       // tacnost (sigurnost)
	TimeSeconds   uint          // vreme u sekundama od 1.1.1970
}
