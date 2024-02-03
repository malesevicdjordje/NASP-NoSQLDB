package structures

import "hash"

type BloomFilter struct {
	Set         []byte        // set sa bitovima
	hashes      []hash.Hash32 // slice hash funkcija
	K           uint          // broj hash funkcija
	M           uint          // duzina (velicina) bloom filtera
	P           float64       // verovatnoca false-positive
	TimeSeconds uint          // vreme u sekundama od 1.1.1970
}
