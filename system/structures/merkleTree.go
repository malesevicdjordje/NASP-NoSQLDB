package structures

import (
	"crypto/sha1"
	"encoding/hex"
)

// MerkleRoot represents the root of the Merkle tree.
type MerkleRoot struct {
	TopNode *MerkleNode
}

func (mr *MerkleRoot) String() string {
	return mr.TopNode.String()
}

// MerkleNode represents a node in the Merkle tree.
type MerkleNode struct {
	HashValue [20]byte
	Left      *MerkleNode
	Right     *MerkleNode
}

func (n *MerkleNode) String() string {
	return hex.EncodeToString(n.HashValue[:])
}

// CalculateHash calculates the SHA-1 hash of the given data.
func CalculateHash(data []byte) [20]byte {
	return sha1.Sum(data)
}

// ConvertStringsToBytes converts an array of strings to a 2D byte slice.
func ConvertStringsToBytes(strings []string) [][]byte {
	byteData := make([][]byte, len(strings))
	for i, str := range strings {
		byteData[i] = []byte(str)
	}
	return byteData
}

// BuildMerkleTree is the entry point for creating the Merkle tree.
func BuildMerkleTree(dataKeys [][]byte, filePath string) *MerkleRoot {
	leafNodes := CreateLeafNodes(dataKeys)

}

// CreateLeafNodes forms leaf nodes of the tree.
func CreateLeafNodes(data [][]byte) []*MerkleNode {
	leaves := make([]*MerkleNode, len(data))
	for i, datum := range data {
		leaves[i] = &MerkleNode{HashValue: CalculateHash(datum), Left: nil, Right: nil}
	}
	return leaves
}
