package structures

import "encoding/hex"

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
