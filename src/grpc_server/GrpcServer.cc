/* Copyright (C) 2025, The Linux Box Corporation
 * Contributor : Avani Rateria <arateria@redhat.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * -------------
 */

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <thread>
#include <mutex>
#include "gRPC/GrpcServer.h"
#include "nfsService.grpc.pb.h"
#include "nfsService.h"
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#define GRPCERROR(MESSAGE)                                                    \
        fprintf(stderr, "[%s:%d] %s: %s\n", __FILE__, __LINE__, (MESSAGE), \
                strerror(errno))
#define GRPCFATAL(MESSAGE) (GRPCERROR(MESSAGE), abort())

/* start and stop grpc server*/
class GrpcServer {
    public:
            GrpcServer();
            void start(uint16_t port);
            void stop(void);
	    std::thread server_thread_;
            ~GrpcServer();

    private:
        bool running_ = false;
        std::mutex mutex_;

        // Delete copy/move constructor/assignment
        GrpcServer(const GrpcServer &) = delete;
        GrpcServer &operator=(const GrpcServer &) = delete;
        GrpcServer(GrpcServer &&) = delete;
        GrpcServer &operator=(GrpcServer &&) = delete;

        std::unique_ptr<grpc::Server> server_;
} ganesha_grpc_server;

GrpcServer::GrpcServer() : running_(false) {}

// stop gRPC server
GrpcServer::~GrpcServer() {
    stop();
}

// start gRPC server
void GrpcServer::start(uint16_t port)
{
        const std::lock_guard<std::mutex> lock(mutex_);
        if (running_)
                GRPCFATAL("Already running");

	std::string server_address("0.0.0.0:" + std::to_string(port));

    	grpc::ServerBuilder builder;
    	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

	// Register the service with the builder
	GetClientIdService showClientService;
	builder.RegisterService(&showClientService);

	GetNfsGraceService nfsIngrace;
	builder.RegisterService(&nfsIngrace);

	GetSessionIdService getClientSessionIds;
	builder.RegisterService(&getClientSessionIds);

	StartNfsGraceService startNfsGrace;
	builder.RegisterService(&startNfsGrace);

	// For grpc CLI
	grpc::reflection::InitProtoReflectionServerBuilderPlugin();
	server_ = builder.BuildAndStart();
	if (!server_) {
		GRPCFATAL(("Failed to start server on %s" + server_address).c_str());
	}
    	running_ = true;
	server_->Wait();
}

void GrpcServer::stop()
{
        const std::lock_guard<std::mutex> lock(mutex_);
        if (running_) {
                running_ = false;
                server_->Shutdown();
		if (server_thread_.joinable()) {
			server_thread_.join();  // Wait for the server thread to finish
		}
        }
}

extern "C" {

void grpc__init(uint16_t port)
{
        static bool initialized = false;
        if (initialized)
                return;
        ganesha_grpc_server.server_thread_ = std::thread([port]() {ganesha_grpc_server.start(port);});
        initialized = true;
}

} /* extern C */

