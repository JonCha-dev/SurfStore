package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
)

/*
Implement the logic for a client syncing with the server here.
*/
func ClientSync(client RPCClient) {
	// get all file hashlist in base dir, make map
	// compare map to local index.txt, make if doesn't exist
	// -- check for new/modified files
	// -- check if file no longer exists (deleted file)
	// get FileInfoMap from server
	// -- compare local with remote

	localFMDMap := make(map[string]FileMetaData)
	localBlockMap := make(map[string]Block)

	// read base directory
	filesInfo, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Printf("32: ")
		log.Fatal(err)
	}

	// populate local FMD/Block maps
	for _, fileInfo := range filesInfo {

		// get filename
		fileName := fileInfo.Name()
		log.Println(fileName)

		// do not add index.txt, cannot contain commas/forwardslash
		if fileName != "index.txt" && !strings.Contains(fileName, "/") && !strings.Contains(fileName, ",") {
			fmd := new(FileMetaData)
			file, err := os.Open(client.BaseDir + "/" + fileName)
			if err != nil {
				log.Printf("46: ")
				log.Fatal(err)
			}

			hashList := []string{}
			// get each block in file and its hash
			for {
				b := new(Block)
				// get BlockSize bytes
				buf := make([]byte, client.BlockSize)
				size, err := file.Read(buf)
				if err != nil {
					log.Printf("59: %s", err)
					break
				}

				// get hashvalue, add to list
				hashBytes := sha256.Sum256(buf[:size])
				hashString := hex.EncodeToString(hashBytes[:])
				hashList = append(hashList, hashString)

				// set block params
				b.BlockData = buf[:size]
				b.BlockSize = size

				// add block to local map
				localBlockMap[hashString] = *b
			}

			// set fmd params
			fmd.Filename = fileName
			fmd.Version = 1
			fmd.BlockHashList = hashList

			// add file to local filemetadata map
			localFMDMap[fileName] = *fmd

			// close file
			file.Close()
		}
	}

	file, err := os.Open(client.BaseDir + "/index.txt")

	// index.txt exists
	if err == nil {
		remaining := ""
		// compare local files with index.txt
		for {
			buf := make([]byte, 100)
			size, err := file.Read(buf)
			// nothing to read
			if err != nil {
				break
			}
			data := buf[:size]
			remaining = remaining + string(data)

			// parse each line
			for strings.Contains(remaining, "\n") {
				newlineIdx := strings.Index(remaining, "\n")
				line := remaining[:newlineIdx]

				// get filename, update remaining line
				idx := strings.Index(line, ",")
				iFileName := line[:idx]
				line = line[idx+1:]

				// get version, update remaining line
				idx = strings.Index(line, ",")
				iVersion, _ := strconv.Atoi(line[:idx])
				line = line[idx+1:]

				// get hashlist from rest of line
				iHashList := []string{}
				for strings.Contains(line, " ") {
					idx = strings.Index(line, " ")
					hashString := line[:idx]
					iHashList = append(iHashList, hashString)
					line = line[idx+1:]
				}

				// compare with localFMDmap
				// file exists in directory
				if fmd, ok := localFMDMap[iFileName]; ok {
					// no modifications
					if reflect.DeepEqual(fmd.BlockHashList, iHashList) {
						fmd.Version = iVersion
						localFMDMap[iFileName] = fmd
						// modifications made
					} else {
						fmd.Version = iVersion + 1
						localFMDMap[iFileName] = fmd
					}

					// file has been deleted
				} else {
					deletedFMD := new(FileMetaData)
					deletedFMD.Filename = iFileName
					deletedFMD.Version = iVersion + 1
					deletedFMD.BlockHashList = append(deletedFMD.BlockHashList, "0")

					// add tombstone ver to localFMDmap
					localFMDMap[iFileName] = *deletedFMD
				}

				// update remaining read bytes
				remaining = remaining[newlineIdx+1:]
			}
		}

		// close the file
		file.Close()
	}

	// get FileInfoMap from server
	var serverFMDMap map[string]FileMetaData
	var someBool bool
	client.GetFileInfoMap(&someBool, &serverFMDMap)

	// get blockstoreaddr
	var someSlice []string
	var blockStoreMap map[string][]string
	var blockStoreAddr string
	client.GetBlockStoreMap(someSlice, &blockStoreMap)
	// only one blockstore
	for k, _ := range blockStoreMap {
		blockStoreAddr = k
		break
	}

	// compare files in remote index with local
	for _, serverFMD := range serverFMDMap {
		fileDeleted := false
		// file in remote index but not local
		if fmd, ok := localFMDMap[serverFMD.Filename]; !ok {
			// get blocks on server
			blocks := []Block{}
			for _, hash := range serverFMD.BlockHashList {
				b := new(Block)
				err := client.GetBlock(hash, blockStoreAddr, b)
				if err != nil {
					log.Printf("188: ")
					log.Fatal(err)
				}

				blocks = append(blocks, *b)
			}
			// create file in base dir, populate with blockdata
			file, err = os.Create(client.BaseDir + "/" + serverFMD.Filename)
			if err != nil {
				log.Printf("197: ")
				log.Fatal(err)
			}
			for _, block := range blocks {
				log.Printf("202")
				file.Write(block.BlockData)
			}

			// close the file
			file.Close()

			// add new fmd to localFMDmap
			localFMDMap[serverFMD.Filename] = serverFMD

			// file in remote and local index
		} else {
			// put blocks on server
			for _, hash := range fmd.BlockHashList {
				client.PutBlock(localBlockMap[hash], blockStoreAddr, &someBool)
			}

			// update the file on server
			var latestVer int
			err = client.UpdateFile(&fmd, &latestVer)

			// version mismatch
			if err != nil {
				log.Printf("222: %s\n", err)
				// get blocks on server
				client.GetFileInfoMap(&someBool, &serverFMDMap)
				serverFMD = serverFMDMap[fmd.Filename]

				blocks := []Block{}
				for _, hash := range serverFMD.BlockHashList {
					if hash != "0" {
						b := new(Block)
						err := client.GetBlock(hash, blockStoreAddr, b)
						if err != nil {
							log.Printf("230: ")
							log.Fatal(err)
						}

						blocks = append(blocks, *b)
						// file deleted
					} else {
						os.Remove(client.BaseDir + "/" + serverFMD.Filename)
						fileDeleted = true
						break
					}
				}

				if !fileDeleted {
					// truncate file in base dir, populate with blockdata
					file, err = os.Create(client.BaseDir + "/" + serverFMD.Filename)
					if err != nil {
						log.Printf("240: ")
						log.Fatal(err)
					}
					for _, block := range blocks {
						log.Println("245")
						file.Write(block.BlockData)
					}

					// close the file
					file.Close()
				}
				// add new fmd to localFMDmap
				localFMDMap[serverFMD.Filename] = serverFMD
			}

		}
	}

	// compare files in local index with remote
	for _, localFMD := range localFMDMap {
		fileDeleted := false
		// file in local index but not remote
		if fmd, ok := serverFMDMap[localFMD.Filename]; !ok {
			// put blocks on server
			for _, hash := range localFMD.BlockHashList {
				client.PutBlock(localBlockMap[hash], blockStoreAddr, &someBool)
			}
			// update file with new fmd
			var latestVer int
			err = client.UpdateFile(&localFMD, &latestVer)

			// if failed (race cond.), download file from server
			if err != nil {
				log.Printf("270: %s", err)
				// get blocks on server
				client.GetFileInfoMap(&someBool, &serverFMDMap)
				fmd = serverFMDMap[localFMD.Filename]
				blocks := []Block{}
				var b Block
				for _, hash := range fmd.BlockHashList {
					if hash != "0" {
						err := client.GetBlock(hash, blockStoreAddr, &b)
						if err != nil {
							log.Printf("279: ")
							log.Fatal(err)
						}

						blocks = append(blocks, b)
						// file deleted
					} else {
						os.Remove(client.BaseDir + "/" + fmd.Filename)
						fileDeleted = true
						break
					}
				}

				if !fileDeleted {
					// truncate file in base dir, populate with blockdata
					file, err = os.Create(client.BaseDir + "/" + localFMD.Filename)
					if err != nil {
						log.Printf("289: ")
						log.Fatal(err)
					}
					for _, block := range blocks {
						log.Println("294")
						file.Write(block.BlockData)
					}

					// close the file
					file.Close()
				}
				// add new fmd to localFMDmap
				localFMDMap[localFMD.Filename] = fmd
			}
		}
	}

	// get final FileInfoMap from server
	var finalFMDMap map[string]FileMetaData
	client.GetFileInfoMap(&someBool, &finalFMDMap)

	// truncate index.txt or create if not exist
	file, _ = os.Create(client.BaseDir + "/index.txt")

	// populate index.txt with updated FMD
	for _, fmd := range finalFMDMap {
		// write file name and version
		file.Write([]byte(fmd.Filename + "," + strconv.Itoa(fmd.Version) + ","))

		// write hashlist
		for _, hash := range fmd.BlockHashList {
			file.Write([]byte(hash + " "))
		}

		// newline
		file.Write([]byte("\n"))
	}

	// close the file
	file.Close()
}

/*
Helper function to print the contents of the metadata map.
*/
func PrintMetaMap(metaMap map[string]FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version, filemeta.BlockHashList)
	}

	fmt.Println("---------END PRINT MAP--------")

}
