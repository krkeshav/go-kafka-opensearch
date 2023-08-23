package main

import (
	"context"
	"crypto/tls"
	"log"
	"net/http"
	"strings"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

type openSearchHelper struct {
	ctx    context.Context
	client *opensearch.Client
}

func NewOpenSearchHelper(ctx context.Context, address []string, inSecure bool) *openSearchHelper {
	openSearchConfig := opensearch.Config{
		Addresses: address,
	}
	if inSecure {
		openSearchConfig.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}
	client, err := opensearch.NewClient(openSearchConfig)
	if err != nil {
		log.Fatal(err)
	}
	return &openSearchHelper{
		ctx:    ctx,
		client: client,
	}
}

func (o *openSearchHelper) CreateIndex(indexName, indexSettings string) bool {
	indexExists := opensearchapi.IndicesExistsRequest{
		Index: []string{indexName},
	}
	eresp, err := indexExists.Do(o.ctx, o.client)
	if err == nil && eresp.StatusCode == 200 {
		return true
	}
	settings := strings.NewReader(indexSettings)
	indexCreateRequest := opensearchapi.IndicesCreateRequest{
		Index: indexName,
		Body:  settings,
	}
	resp, err := indexCreateRequest.Do(o.ctx, o.client)
	if err != nil {
		log.Println(err)
		return false
	}
	if resp.StatusCode != 200 {
		log.Println("Didnt receive 200 status", resp.StatusCode)
		return false
	}

	return true
}

func (o *openSearchHelper) IndexDocument(indexName, docId, docBody string) bool {
	document := strings.NewReader(docBody)
	req := opensearchapi.IndexRequest{
		Index:      indexName,
		DocumentID: docId,
		Body:       document,
	}
	resp, err := req.Do(o.ctx, o.client)
	if err != nil {
		log.Println(err)
		return false
	}
	if resp.StatusCode != 201 {
		log.Println("Didnt receive 201 status", resp.StatusCode)
		return false
	}
	return true
}
