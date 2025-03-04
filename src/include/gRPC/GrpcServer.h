/**
 * @brief gRPC library for NFS Ganesha.
 */

#ifndef GANESHA_GRPC_H
#define GANESHA_GRPC_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Inits grpc module. */
void grpc__init(uint16_t port);

#ifdef __cplusplus
}
#endif
#endif /* GANESHA_GRPC_H */
