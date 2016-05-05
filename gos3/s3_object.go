package gos3

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
)

type (
	// Bucket is s3 bucket object
	Bucket struct {
		Path string
		Name string
	}

	// Key is s3 key object
	Key struct {
		Bucket Bucket
		Name   string
	}

	// Metadata is s3 metadata
	Metadata struct {
		Digest        string `json:"digest"`
		ContentLength int    `json:"content_length"`
		ContentType   string `json:"content_type"`
	}
)

func (b *Bucket) Dir() string {
	return path.Join(b.Path, b.Name)
}

func (b *Bucket) Create() error {
	return os.MkdirAll(b.Dir(), 0755)
}

func (b *Bucket) NewKey() *Key {
	return &Key{Bucket: *b}
}

func (b *Bucket) Lookup(name string) *Key {
	return &Key{Bucket: *b, Name: name}
}

func (k *Key) MetadataPath() string {
	return path.Join(k.Dir(), "metadata.json")
}

func (k *Key) ContentPath() string {
	return path.Join(k.Dir(), "content")
}

func (k *Key) Metadata() *Metadata {
	filepath := k.MetadataPath()
	if _, err := os.Stat(filepath); err != nil {
		return nil
	}

	var metadata Metadata
	file, _ := os.Open(filepath)
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.Decode(&metadata)

	return &metadata
}

func (k *Key) Dir() string {
	return path.Join(k.Bucket.Dir(), k.Name)
}

func (k *Key) IsStored() bool {
	_, err := os.Stat(k.Dir())
	return !os.IsNotExist(err)
}

func (k *Key) Save(data io.Reader, contentType string) (string, error) {
	if err := os.MkdirAll(k.Dir(), 0755); err != nil {
		return "", err
	}
	content := StreamToByte(data)
	ioutil.WriteFile(k.ContentPath(), content, os.ModePerm)
	digest := GetMD5Digest(content)

	meta := &Metadata{
		Digest:        digest,
		ContentType:   contentType,
		ContentLength: len(content)}

	b, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}

	f, err := os.Create(k.MetadataPath())
	if err != nil {
		return "", err
	}

	defer f.Close()

	f.WriteString(string(b))
	f.Sync()

	return digest, nil
}

func (k *Key) Content() (*os.File, error) {
	if !k.IsStored() {
		return nil, errors.New("metadata error")
	}
	return os.Open(k.ContentPath())
}

func (k *Key) Delete() error {
	return os.RemoveAll(k.Dir())
}
