package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

//this function will return the name of last bucket.
func getBuckets(client *s3.Client) (bucketName string) {

	output, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		panic(err)
	}
	for _, bucket := range output.Buckets {
		fmt.Println(*bucket.Name)
		bucketName = *bucket.Name
		// TODO
		// change code to return
	}
	return bucketName
}

func getObjects(client *s3.Client, bucketName string) (key string) {
	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("first page results of " + bucketName + " : ")
	for _, object := range output.Contents {
		log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
		key = aws.ToString(object.Key)
	}

	return key
}

func createBucket(client *s3.Client, bucketName string, region types.BucketLocationConstraint) {
	// 버킷 생성하기
	output, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: &bucketName,
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: region,
		},
	})

	if err != nil {
		panic(err)
	}
	fmt.Println(output.Location)
}

func downloadFile(client *s3.Client, bucketName string, path string, key string) error {
	// Create the directories in the path
	splitKeyArr := strings.Split(key, "/")
	file := filepath.Join(path, splitKeyArr[len(splitKeyArr)-1])
	if err := os.MkdirAll(filepath.Dir(file), 0775); err != nil {
		return err
	}

	// Set up the local file
	fd, err := os.Create(file)
	if err != nil {
		return err
	}

	defer fd.Close()

	downloader := manager.NewDownloader(client)
	_, err = downloader.Download(context.TODO(), fd,
		&s3.GetObjectInput{
			Bucket: &bucketName,
			Key:    &key,
		})
	return err

}

func uploadFile(client *s3.Client, bucketName string, fileName string) *manager.UploadOutput {

	file, err := ioutil.ReadFile(fileName)
	uploader := manager.NewUploader(client)
	result, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fileName),
		Body:   bytes.NewReader(file),
	})
	if err != nil {
		log.Fatal(err)
		panic(err)
	}

	return result

}
func main() {
	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-northeast-2"))
	if err != nil {
		log.Fatal(err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)

	var bucketName = getBuckets(client)

	key := getObjects(client, bucketName)

	println(key)

	//downloadFile(client, bucketName, "./", key)
	//createBucket(client, "testbucket4885", "ap-northeast-2")
	println(uploadFile(client, bucketName, "test2.png"))

}
