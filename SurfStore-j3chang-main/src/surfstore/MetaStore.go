package surfstore

import "fmt"

type MetaStore struct {
	FileMetaMap map[string]FileMetaData
	BlockStores []string
}

func (m *MetaStore) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {
	*serverFileInfoMap = m.FileMetaMap
	return nil
}

func (m *MetaStore) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error) {
	if fileMeta, ok := m.FileMetaMap[fileMetaData.Filename]; !ok {
		// new file
		m.FileMetaMap[fileMetaData.Filename] = *fileMetaData
		*latestVersion = fileMetaData.Version
	} else {
		// existing file
		if fileMetaData.Version == (fileMeta.Version + 1) {
			// correct version store
			m.FileMetaMap[fileMetaData.Filename] = *fileMetaData
			*latestVersion = fileMetaData.Version
		} else {
			// incorrect version
			*latestVersion = fileMeta.Version
			return fmt.Errorf("error, requires version %d, your version: %d",
				fileMeta.Version+1, fileMetaData.Version)
		}
	}

	return nil
}

func (m *MetaStore) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, bs := range m.BlockStores {
		(*blockStoreMap)[bs] = blockHashesIn
	}

	return nil
}

var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreList []string) MetaStore {
	return MetaStore{map[string]FileMetaData{}, blockStoreList}
}
