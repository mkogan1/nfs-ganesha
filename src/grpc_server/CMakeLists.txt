# SPDX-License-Identifier: LGPL-3.0-or-later
#-------------------------------------------------------------------------------
#
# Copyright Panasas, 2025
# Contributor:  Avani Rateria <arateria@redhat>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
#-------------------------------------------------------------------------------
# gRPC server  Cmake

include_directories(${PROTOBUF_INCLUDE_DIRS})
include_directories(${gRPC_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_BINARY_DIR}/proto)

set(SOURCE_FILES ${CMAKE_SOURCE_DIR}/grpc_server/GrpcServer.cc
    ${CMAKE_BINARY_DIR}/proto/nfsService.pb.cc
    ${CMAKE_BINARY_DIR}/proto/nfsService.grpc.pb.cc
	${CMAKE_SOURCE_DIR}/grpc_server/nfsServiceClient.cpp
	${CMAKE_SOURCE_DIR}/grpc_server/nfsServiceServer.cpp)

add_library(gRPCLibrary OBJECT ${SOURCE_FILES})
target_link_libraries(gRPCLibrary
                      PRIVATE
                      gRPC::grpc++
                      protobuf::libprotobuf
		      grpc++_reflection
	      	      ${SYSTEM_LIBRARIES}
	      		sal)
		target_include_directories(gRPCLibrary PRIVATE
	                  ${PROTOBUF_INCLUDE_DIR}
	                   ${CMAKE_SOURCE_DIR}/proto
	         	   ${CMAKE_SOURCE_DIR}/grpc_server)
set_target_properties(gRPCLibrary PROPERTIES COMPILE_FLAGS "-fPIC")
install(TARGETS gRPCLibrary LIBRARY DESTINATION ${LIB_INSTALL_DIR})

