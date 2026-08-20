package health

import (
	"context"

	"google.golang.org/grpc/health/grpc_health_v1"
)

type HealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	status grpc_health_v1.HealthCheckResponse_ServingStatus
}

func NewHealthServer() *HealthServer {
	return &HealthServer{
		status: grpc_health_v1.HealthCheckResponse_SERVING,
	}
}

func (s *HealthServer) SetStatus(status grpc_health_v1.HealthCheckResponse_ServingStatus) {
	s.status = status
}

func (s *HealthServer) Check(
	ctx context.Context,
	req *grpc_health_v1.HealthCheckRequest,
) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{
		Status: s.status,
	}, nil
}
