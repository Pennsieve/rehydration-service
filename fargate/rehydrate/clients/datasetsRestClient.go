package clients

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type DatasetRestClient struct {
	Client  *http.Client
	BaseURL string
	Method  string
}

func NewDatasetRestClient(client *http.Client, baseURL string, method string) Client {
	return &DatasetRestClient{client, baseURL, method}
}

func (d *DatasetRestClient) GetDatasetByVersion(ctx context.Context, datasetId string, versionId string) ([]byte, error) {
	endpoint := fmt.Sprintf("%s/%s/versions/%s",
		d.BaseURL, datasetId, versionId)
	requestDuration := 30 * time.Second
	return d.Execute(ctx, nil, endpoint, requestDuration)
}

func (d *DatasetRestClient) GetDatasetMetadata(ctx context.Context, datasetId string, versionId string) ([]byte, error) {
	endpoint := fmt.Sprintf("%s/%s/versions/%s/metadata",
		d.BaseURL, datasetId, versionId)
	requestDuration := 30 * time.Second

	return d.Execute(ctx, nil, endpoint, requestDuration)
}

func (d *DatasetRestClient) Execute(ctx context.Context, b io.Reader, endpoint string, requestDuration time.Duration) ([]byte, error) {
	req, err := http.NewRequest(d.Method, endpoint, b)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	requestContext, cancel := context.WithTimeout(ctx, requestDuration)
	defer cancel()
	req = req.WithContext(requestContext)
	resp, err := d.Client.Do(req)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	defer resp.Body.Close()
	s, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println(err.Error())
		return s, err
	}
	return s, nil
}
