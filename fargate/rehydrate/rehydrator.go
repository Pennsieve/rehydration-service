package main

import (
	"log"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Process interface {
	Copy(Src, Dest) error
}

// source
type Src interface {
	GetUri() string
	GetFileSize() int64
	GetFilename() string
}

type SrcObject struct {
	SourceBucketUri string
	FileSize        int64
	Filename        string
}

func (s SrcObject) GetUri() string {
	return s.SourceBucketUri
}

func (s SrcObject) GetFileSize() int64 {
	return s.FileSize
}

func (s SrcObject) GetFilename() string {
	return s.Filename
}

// destination
type Dest interface {
	GetUri() string
}

type DestObject struct {
	DestinationBucketUri string
}

func (d DestObject) GetUri() string {
	return d.DestinationBucketUri
}

// rehydration
type Rehydration struct {
	Src  Src
	Dest Dest
}

func NewRehydration(src SrcObject, dest DestObject) *Rehydration {
	return &Rehydration{src, dest}
}

// rehydration process
type Rehydrator struct {
	S3 *s3.Client
}

func NewRehydrator(s3 *s3.Client) S3Process {
	return &Rehydrator{s3}
}

func (r *Rehydrator) Copy(src Src, dest Dest) error {
	// TODO: file is less than or equal to 100MB ? simple copy : multiPart copy
	log.Printf("copying %s (size: %v) from %s to %s",
		src.GetFilename(), src.GetFileSize(), src.GetUri(), dest.GetUri())
	return nil
}
