package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/ipfs/go-datastore"
	"github.com/textileio/powergate/v2/index/miner"
)

var (
	dsMetadata      = datastore.NewKey("meta")
	dsOnChain       = datastore.NewKey("onchain")
	dsOnChainHeight = datastore.NewKey("height")
)

// Store is a store to save on-chain and metadata information about the chain.
type Store struct {
	ds datastore.Datastore
}

// New returns a new *Store.
func New(ds datastore.Datastore) (*Store, error) {
	return &Store{
		ds: ds,
	}, nil
}

// SaveMetadata creates/updates metadata information of miners.
func (s *Store) SaveMetadata(index miner.MetaIndex) error {
	for addr, meta := range index.Info {
		key := makeMinerMetadataKey(addr)
		buf, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshaling metadata for miner %s: %s", addr, err)
		}
		if err := s.ds.Put(key, buf); err != nil {
			return fmt.Errorf("saving metadata in store: %s", err)
		}
	}
	return nil
}

// SaveOnChain creates/updates on-chain information of miners.
func (s *Store) SaveOnChain(index miner.ChainIndex) error {
	for addr, onchain := range index.Miners {
		key := makeMinerOnChainKey(addr)
		buf, err := json.Marshal(onchain)
		if err != nil {
			return fmt.Errorf("marshaling onchain for miner %s: %s", addr, err)
		}
		if err := s.ds.Put(key, buf); err != nil {
			return fmt.Errorf("saving onchain in store: %s", err)
		}
	}

	key := makeOnChainHeightKey(index.LastUpdated)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(index.LastUpdated))
	if err := s.ds.Put(key, buf); err != nil {
		return fmt.Errorf("saving metadata in store: %s", err)
	}

	return nil
}

func makeMinerMetadataKey(addr string) datastore.Key {
	return dsMetadata.ChildString(addr)
}

func makeMinerOnChainKey(addr string) datastore.Key {
	return dsOnChain.ChildString(addr)
}

func makeOnChainHeightKey(epoch int64) datastore.Key {
	return dsOnChain.Child(dsOnChainHeight).ChildString(strconv.FormatInt(epoch, 10))
}
