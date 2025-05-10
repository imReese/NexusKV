package health

import "google.golang.org/grpc/health/grpc_health_v1"

type HealthServer struct {
    grpc_health_v1.UnimplementedHealthServer
}

func (s *HealthServer) Check(ctx context.Context, 
    req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
    // 实现健康检查逻辑
}