#SPDX - License - Identifier : LGPL - 3.0 - or -later
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
#
#Copyright IBM, 2025
#Contributor : Avani Rateria < arateria @redhat>
#
#This program is free software; you can redistribute it and / or
#modify it under the terms of the GNU Lesser General Public
#License as published by the Free Software Foundation; either
#version 3 of the License, or (at your option) any later version.
#
#This program is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the GNU
#Lesser General Public License for more details.
#
#You should have received a copy of the GNU Lesser General Public
#License along with this library; if not, write to the Free Software
#Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110 - 1301 USA
#
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
#Proto Cmake

include_directories(${PROTOBUF_INCLUDE_DIRS})
include_directories(${gRPC_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/nfsService.proto")
set(PROTO_OUT_DIR "${CMAKE_BINARY_DIR}/proto" CACHE PATH "Path to generated proto
    files")

get_target_property(GRPC_CPP_PLUGIN_LOCATION gRPC::grpc_cpp_plugin LOCATION)

set(PROTO_SRCS ${PROTO_OUT_DIR}/nfsService.pb.cc)
set(PROTO_HDRS ${PROTO_OUT_DIR}/nfsService.pb.h)
set(GRPC_SRCS ${PROTO_OUT_DIR}/nfsService.grpc.pb.cc)
set(GRPC_HDRS ${PROTO_OUT_DIR}/nfsService.grpc.pb.h)

execute_process(
    COMMAND protoc --grpc_out=${PROTO_OUT_DIR} --cpp_out=${PROTO_OUT_DIR}
    --experimental_allow_proto3_optional
    --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_LOCATION}
    --proto_path=${CMAKE_SOURCE_DIR}/proto ${PROTO_FILE}
   WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
   RESULT_VARIABLE protoc_result
   OUTPUT_VARIABLE protoc_output
   ERROR_VARIABLE protoc_error
)

if(NOT ${protoc_result} EQUAL 0)
    message(FATAL_ERROR "protoc failed: ${protoc_error}")
    set(USE_GRPC OFF)
else()
    message(STATUS "protoc ran successfully: ${protoc_output}")
endif()

add_library(proto-lib SHARED ${PROTO_SRCS} ${GRPC_SRCS})

target_include_directories(proto-lib PUBLIC ${PROTO_OUT_DIR})
target_link_libraries(proto-lib PUBLIC protobuf::libprotobuf gRPC::grpc++ grpc++_reflection)

set_target_properties(proto-lib PROPERTIES COMPILE_FLAGS "-fPIC")

set_directory_properties(PROPERTIES ADDITIONAL_MAKE_CLEAN_FILES "${PROTO_OUT_DIR}")

install(TARGETS proto-lib LIBRARY DESTINATION ${LIB_INSTALL_DIR})
