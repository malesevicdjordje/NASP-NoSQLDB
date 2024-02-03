package structures

import (
	"math"
)

type HyperLogLog struct {
	P         uint8   //broj vodecih bitova za bucket
	M         uint64  //velicina seta
	Registers []uint8 //set registara
}

func CreateHyperLogLog(numOfLeadingBits uint8) *HyperLogLog {
	sizeOfRegisters := int(math.Pow(2, float64(numOfLeadingBits)))
	return &HyperLogLog{numOfLeadingBits, uint64(sizeOfRegisters), make([]uint8, sizeOfRegisters, sizeOfRegisters)}
}
