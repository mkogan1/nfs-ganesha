include_directories(${PROTOBUF_INCLUDE_DIRS})
include_directories(${gRPC_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

set(SOURCE_FILES ${CMAKE_SOURCE_DIR}/proto/nfsService.proto
            ${CMAKE_SOURCE_DIR}/proto/nfsService.pb.cc
            ${CMAKE_SOURCE_DIR}/proto/nfsService.grpc.pb.cc)

    add_library(protoLibrary SHARED ${SOURCE_FILES})
target_link_libraries(protoLibrary
                      PRIVATE
                      gRPC::grpc++
                      protobuf::libprotobuf
                      grpc++_reflection
                      ${SYSTEM_LIBRARIES})
                target_include_directories(protoLibrary PRIVATE
                          ${PROTOBUF_INCLUDE_DIR}
                           ${CMAKE_SOURCE_DIR}/proto
                           ${CMAKE_SOURCE_DIR}/grpc_server)
install(TARGETS protoLibrary LIBRARY DESTINATION ${LIB_INSTALL_DIR})
