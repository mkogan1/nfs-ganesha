/**
 * @brief gRPC library for NFS Ganesha.
 */

#ifndef GANESHA_GRPC_H
#define GANESHA_GRPC_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <thread>

#ifdef __cplusplus
extern "C" {
#endif

/* Inits grpc module. */
void grpc__init(uint16_t port);

#ifdef __cplusplus
}
#endif

/* start and stop grpc server*/
class GrpcServer {
    public:
	    GrpcServer();
	    void start(uint16_t port);
	    void stop(void);
	    ~GrpcServer();

    private:
	static constexpr int INVALID_FD = -1;
	int server_fd_ = INVALID_FD;
	bool running_ = false;
	std::thread server_thread_;
	std::mutex mutex_;

	// Delete copy/move constructor/assignment
	GrpcServer(const GrpcServer &) = delete;
	GrpcServer &operator=(const GrpcServer &) = delete;
	GrpcServer(GrpcServer &&) = delete;
	GrpcServer &operator=(GrpcServer &&) = delete;

	//static void *server_thread_(void *arg);
	std::unique_ptr<grpc::Server> server_;
};

extern GrpcServer ganesha_grpc_server;

#endif /* GANESHA_GRPC_H */
