package bigbite

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type Config struct {
	awsClient       *s3.Client
	bucketName      *string
	prefix          *string
	listContext     context.Context
	downloadContext context.Context
	numberOfWorkers int
}

// iterate through the bucket
func Bite(c Config) {
	jobs := make(chan *string)
	wg := &sync.WaitGroup{}
	input := &s3.ListObjectsV2Input{
		Bucket:            c.bucketName,
		Prefix:            c.prefix,
		ContinuationToken: nil,
	}

	wg.Add(c.numberOfWorkers)

	newWorker(c, jobs, wg)

	for {
		r, err := c.awsClient.ListObjectsV2(c.listContext, input)

		if err != nil {
			panic(err)
		}

		for _, object := range r.Contents {
			jobs <- object.Key
		}

		input.ContinuationToken = r.NextContinuationToken

		if !r.IsTruncated {
			break
		}
	}

	close(jobs)

	wg.Wait()
}

// create workers to iterate over files
func newWorker(c Config, jobs chan *string, wg *sync.WaitGroup) {
	var result chan string = make(chan string)

	for i := 0; i < c.numberOfWorkers; i++ {
		go fileWorker(c, jobs, result, wg)
	}

	go resultWorker(result, wg)
}

// worker routine, go through an S3 file from a key and find an allowed asset in the file, send it to the results channel if it is
func fileWorker(c Config, jobs chan *string, result chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	d := manager.NewDownloader(c.awsClient)

	for s := range jobs {
		input := s3.GetObjectInput{
			Bucket: c.bucketName,
			Key:    s,
		}

		b := manager.NewWriteAtBuffer([]byte{})
		_, err := d.Download(c.downloadContext, b, &input)

		if err != nil {
			panic(err)
		}

		r := bytes.NewReader(b.Bytes())
		// data, err := gzip.NewReader(r)
		scanner := bufio.NewScanner(r)

		for scanner.Scan() {
			result <- scanner.Text()
		}
	}
}

// write the results to one big file
func resultWorker(results chan string, wg *sync.WaitGroup) {
	f, err := os.OpenFile("results.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0750)

	if err != nil {
		panic(err)
	}

	defer f.Close()

	for l := range results {
		_, err = f.WriteString(l + "\n")

		if err != nil {
			panic(err)
		}
	}
}
