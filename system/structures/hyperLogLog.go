package structures

import (
	"bytes"
	"encoding/gob"
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

func (hll *HyperLogLog) Add(word string) {
	binary := ToBinary(GetMD5Hash(word))
	numOfBucket := 0
	numOfLeadingBits := hll.P
	for i := 0; i < int(numOfLeadingBits); i++ {
		numOfBucket += (int(binary[i]) - '0') * int(math.Pow(2, float64(int(numOfLeadingBits)-i)))
	}
	numOfMostConsecutiveZeros := 0
	for i := len(binary) - 1; i > 0; i-- {
		if binary[i] == '0' {
			numOfMostConsecutiveZeros++
		} else {
			break
		}
	}
	hll.Registers[numOfBucket] = uint8(numOfMostConsecutiveZeros)

}

func (hll *HyperLogLog) Evaluate() float64 {
	harmony := 0.0
	for _, value := range hll.Registers {
		harmony += math.Pow(math.Pow(2.0, float64(value)), -1)
	}

	constant := 0.79402
	estimation := constant * math.Pow(float64(hll.M), 2.0) / harmony
	emptyRegisters := hll.emptyRegistersCount()

	if estimation < 2.5*float64(hll.M) { // small range correction
		if emptyRegisters > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegisters))
		}
	} else if estimation > math.Pow(2.0, 32.0)/30.0 { // large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HyperLogLog) emptyRegistersCount() uint8 {
	numOfEmptyRegisters := uint8(0)
	for _, value := range hll.Registers {
		if value == 0 {
			numOfEmptyRegisters++
		}
	}
	return numOfEmptyRegisters
}

func (hyperLogLog *HyperLogLog) SerializeHLL() []byte {

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	encoder.Encode(&hyperLogLog)
	return buffer.Bytes()
}

func DeserializeHLL(data []byte) *HyperLogLog {

	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	hyperLogLog := new(HyperLogLog)

	for {
		err := decoder.Decode(&hyperLogLog)
		if err != nil {
			break
		}
	}
	return hyperLogLog
}
