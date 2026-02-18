package service

import (
	"context"
	"net/http"
	"time"

	openapi "isbd4/openapi"
)

// MetadataAPIService is a service that implements the logic for the MetadataAPIServicer
// This service should implement the business logic for every endpoint for the MetadataAPI API.
// Include any external packages or services that will be required by this service.
type MetadataAPIService struct {
	startTime int64
}

// NewMetadataAPIService creates a default api service
func NewMetadataAPIService() *MetadataAPIService {
	return &MetadataAPIService{startTime: time.Now().Unix()}
}

// GetSystemInfo - Get basic information about the system (e.g. version, uptime, etc.)
func (s *MetadataAPIService) GetSystemInfo(ctx context.Context) (openapi.ImplResponse, error) {

	sysInfo := openapi.SystemInformation{
		Author:           "Tomasz Głąb",
		Version:          "Lab4.0.0",
		InterfaceVersion: "2.1.0",
		Uptime:           time.Now().Unix() - s.startTime,
	}
	return openapi.Response(http.StatusOK, sysInfo), nil
}
