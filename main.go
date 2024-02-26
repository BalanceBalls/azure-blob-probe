package main

import (
	"bytes"
	"context"
	"flag"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

const EMPTY string = "unspecified"

func main() {
	var sampleBlobName string
	var containerName string
	var connectionString string

	log.Printf("use '-h' for help")

	flag.StringVar(&containerName, "c", EMPTY, "storage container name")
	flag.StringVar(&connectionString, "s", EMPTY, "storage account connection string")
	flag.StringVar(&sampleBlobName , "n", "test-blob", "name for the sample blob (optional)")
	timeoutFlag := flag.Int("t", 30, "probe timeout in seconds (optional)")
	flag.Parse()

	if containerName == EMPTY {
		log.Fatal("Please specify container name. Ex: './probe -c qwe'")
	}

	if connectionString == EMPTY {
		log.Fatal("Please specify SAS token. Ex: './probe -s qwe'")
	}

	log.Println("Probe timeout is set to ", *timeoutFlag, "s")
	log.Println("Using the following blob name: ", sampleBlobName)
	log.Println("Using the following container: ", containerName)
	log.Println("Using the following connection string: ", connectionString)

	client, err := azblob.NewClientFromConnectionString(connectionString, nil)

	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*(time.Duration(*timeoutFlag)))
	defer cancel()
	defer cleanUp(ctx, containerName, sampleBlobName, client)	

	data := []byte("\nHello, world! This is a blob.\n")

	log.Println("Uploading a blob named ", sampleBlobName)
	_, err = client.UploadBuffer(ctx, containerName, sampleBlobName, data, &azblob.UploadBufferOptions{})

	if err != nil {
		log.Fatal("Could not upload a sample file to blob:", err)
	}

	log.Printf("Sample file has been uploaded. Container - %s ; Blob name - %s \n", containerName, sampleBlobName)
	log.Println("Attempting to download newly created blob...")

	var fileBuffer []byte = make([]byte, len(data))
	opCode, err := client.DownloadBuffer(ctx, containerName, sampleBlobName, fileBuffer, &azblob.DownloadBufferOptions{})

	if err != nil {
		log.Fatal("Failed to download blob: ", err)
	}

	log.Println("Blob download ended with code ", opCode)

	if !bytes.Equal(data, fileBuffer) {
		log.Fatal("File integrity check failed. Probe finished with failure")
	} 

	log.Println("File integrity check successful. Probe finished with success")
}

func cleanUp(ctx context.Context, container, blob string, client *azblob.Client) {
	log.Println("Beginning a clean up....")

	_, err := client.DeleteBlob(ctx, container, blob, &azblob.DeleteBlobOptions{})	

	if err != nil {
		log.Println("Failed to remove sample blob. Reason: ", err)
		return
	}

	log.Printf("Removed sample blob '%s' from container '%s' \n", blob, container)
}
