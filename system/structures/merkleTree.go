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
	rootNode := CreateAllNodes(leafNodes)
}

// CreateLeafNodes forms leaf nodes of the tree.
func CreateLeafNodes(data [][]byte) []*MerkleNode {
	leaves := make([]*MerkleNode, len(data))
	for i, datum := range data {
		leaves[i] = &MerkleNode{HashValue: CalculateHash(datum), Left: nil, Right: nil}
	}
	return leaves
}

// CreateAllNodes creates all levels of the tree from leaves to root.
func CreateAllNodes(leafNodes []*MerkleNode) *MerkleNode {
	levelNodes := leafNodes

	for len(levelNodes) > 1 {
		parentNodes := make([]*MerkleNode, 0, len(levelNodes)/2)

		for i := 0; i < len(levelNodes); i += 2 {
			node1 := levelNodes[i]
			node2 := getOrCreateEmptyNode(levelNodes, i+1)
			parent := createParentNode(node1, node2)
			parentNodes = append(parentNodes, parent)
		}

		levelNodes = parentNodes
	}

	return levelNodes[0]
}

func createParentNode(node1, node2 *MerkleNode) *MerkleNode {
	node1Data := node1.HashValue[:]
	node2Data := node2.HashValue[:]
	newNodeBytes := append(node1Data, node2Data...)
	newNode := &MerkleNode{HashValue: CalculateHash(newNodeBytes), Left: node1, Right: node2}
	return newNode
}

func getOrCreateEmptyNode(nodes []*MerkleNode, index int) *MerkleNode {
	if index < len(nodes) {
		return nodes[index]
	}
	return &MerkleNode{HashValue: [20]byte{}, Left: nil, Right: nil}
}
