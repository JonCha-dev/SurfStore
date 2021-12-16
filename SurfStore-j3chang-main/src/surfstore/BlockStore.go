package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type BlockStore struct {
	BlockMap map[string]Block
}

func (bs *BlockStore) GetBlock(blockHash string, blockData *Block) error {
	if _, ok := bs.BlockMap[blockHash]; !ok {
		return fmt.Errorf("block not found in map, hash: %s", blockHash)
	}

	*blockData = bs.BlockMap[blockHash]

	return nil
}

func (bs *BlockStore) PutBlock(block Block, succ *bool) error {
	hashBytes := sha256.Sum256(block.BlockData)
	hashString := hex.EncodeToString(hashBytes[:])

	bs.BlockMap[hashString] = block
	*succ = true

	return nil
}

func (bs *BlockStore) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	for _, hashString := range blockHashesIn {
		if _, ok := bs.BlockMap[hashString]; ok {
			*blockHashesOut = append(*blockHashesOut, hashString)
		}
	}
	return nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() BlockStore {
	return BlockStore{BlockMap: map[string]Block{}}
}
