/*
 * =====================================================================================
 *
 *       Filename:  nfs_qosmgr.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  11/22/2024 02:02:29 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
void dbus_qosmgr_init(void);
struct QoS_perClient_Class *get_client_qos(const sockaddr_t *client_ip);
struct QoS_perShare_Class *get_share_qos(struct gsh_export *export);
bool set_pspc_bandwidth(sockaddr_t *client_ip, struct gsh_export *export,
			uint64_t read_bw, uint64_t write_bw);
bool get_pspc_bandwidth(sockaddr_t *client_ip, struct gsh_export *export,
			uint64_t *read_bw, uint64_t *write_bw);
bool get_pspc_tokens(sockaddr_t *client_ip, struct gsh_export *export,
		     uint64_t *max_tokens, uint64_t *token_renewal);
bool set_pspc_tokens(sockaddr_t *client_ip, struct gsh_export *export,
		     uint64_t max_tokens, uint64_t token_renewal);
uint32_t get_share_client_count(struct QoS_perShare_Class *s_qos_class);
