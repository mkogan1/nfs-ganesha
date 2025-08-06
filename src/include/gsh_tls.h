
#ifdef USE_TLS
/* TLS configuration structure */
typedef struct gsh_tls_config {
	bool enabled;
	char *cert_file;
	char *key_file;
	char *ca_file;
	char *ciphers;
	char *min_version;
	time_t session_timeout;
	bool ktls;          /* Enable kernel TLS if available */
	bool debug;          /* for enabling debug */
} gsh_tls_config_t;

extern gsh_tls_config_t tls_config;
extern struct config_block tls_core;
extern bool nfs_init_tls(gsh_tls_config_t from_ganesha);
#endif /* GSH_TLS_H */
