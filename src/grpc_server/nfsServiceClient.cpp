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

#include <iostream>
#include <grpcpp/grpcpp.h>
#include <nfsService.grpc.pb.h>
#include <string>

void GetClientIds( const std::string& server_address) {
    // Creating a channel to communicate with the server
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
    std::unique_ptr<nfsService::GetClientId::Stub> stub = nfsService::GetClientId::NewStub(channel);

    // Creating a request and response
    nfsService::GetClientIdsRequest request;
    nfsService::GetClientIdsResponse response;
    grpc::ClientContext context;

    // Make the gRPC call
    grpc::Status status = stub->GetClientIds(&context, request, &response);

    if (status.ok()) {
        std::cout << "Client IDs: ";
        for (int i = 0; i < response.client_ids_size(); ++i) {
            //LogWarn(COMPONENT_GRPC, "%s", errormsg);
		std::cout << response.client_ids(i) << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "gRPC call failed: " << status.error_message() << std::endl;
    }
}

void GetNfsGracePeriod(const std::string& server_address) {
	// Creating a channel to communicate with the server
	std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
	std::unique_ptr<nfsService::GetNfsGrace::Stub> stub = nfsService::GetNfsGrace::NewStub(channel);

	// Creating a request and response
	nfsService::GetNfsGraceRequest request;
	nfsService::GetNfsGraceResponse response;
	grpc::ClientContext context;

	// Make the gRPC call
	grpc::Status status = stub->GetGracePeriod(&context, request, &response);

	if (status.ok()) {
        	std::cout << "NFS in grace: " << response.ingrace();
    	} else {
        	std::cout << "gRPC call failed: " << status.error_message() << std::endl;
    	}

}

void GetClientSessionIds(const std::string& server_address) {
	// Creating a channel to communicate with the server
	std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
	std::unique_ptr<nfsService::GetSessionId::Stub> stub = nfsService::GetSessionId::NewStub(channel);

	// Creating a request and response
	nfsService::GetSessionIdsRequest request;
        nfsService::GetSessionIdsResponse response;
        grpc::ClientContext context;

	// Make the gRPC call
        grpc::Status status = stub->GetSessionIds(&context, request, &response);

	if (status.ok()) {
		std::cout << "GetSessionIds response received:" << std::endl;
        	for (const std::string& session_id : response.session_ids()) {
            	std::cout << session_id << std::endl;
        }
    	} else {
        	std::cerr << "RPC failed with status: " << status.error_message() << std::endl;
    	}

}
