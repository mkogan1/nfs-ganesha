#include <time.h>

#define IS_QOS_IO (1 << 0)
#define IS_QOS_IO_READ_BYPASS (1 << 1)

#define NON_RATELIMITING_IO 0
#define RATELIMITING_IO 1

#define QOS_NOT_ENABLED 0
#define QOS_PS_ENABLED 1
#define QOS_PC_ENABLED 2
#define QOS_PS_PC_ENABLED 3

struct qos_op_cb_arg {
	/* caller_data is mainly the write_data and read_data ptr*/
	void *caller_data;
	/* Distingiush the IO for ratelimiting IO or token exhausted IO */
	uint32_t ratecontrol;
};

/* Structure used for handling the BW and token exhausted clients */
typedef struct timer_entry {
	/*  Epirt time of this IO */
	uint64_t expiry;
	/* Size of the IO, required for BW calculation */
	uint64_t size;
	/* Callback function to call on timer expiry */
	void (*callback)(void *);
	/* Call back arg : struct qos_op_cb_arg  */
	void *args;
	struct timer_entry *next;
} timer_entry_t;

/* Currently only NFS4 read IO and write IO are tapped */
enum qos_operation_type { QOS_READ, QOS_WRITE };

/* QOS can be enabled for PS, PC or PSPC */
enum qos_class_type { QOS_SHARE, QOS_CLIENT, QOS_PSPC };

/* Structure used on token exhausted by client */
typedef struct qos_client_entry {
	sockaddr_t *client_addr;
	unsigned int num_ios_waiting;
	bool epoll_disabled;
	timer_entry_t *io_waitlist_qos;
	SVCXPRT *rq_xprt;
	compound_data_t *data;
	struct qos_client_entry *next;
} qos_client_entry_t;

/* Actual struture which holds the accounting.
 * In case of combined, only write accounting structure
 * will be used */
typedef struct qos_bucket {
	pthread_mutex_t lock;
	uint32_t num_ios_waiting;
	/* BW conrtol, io wait queue */
	timer_entry_t *io_waitlist_qos_bc;
	uint64_t max_bw_allowed;
	/* Used to control BW, Last Data Consumed time */
	uint64_t bw_ldct;
	uint64_t tokens_consumed;
	uint64_t max_available_tokens;
	uint64_t tokens_renew_time; /*   In useconds */
	uint64_t last_tokens_consumed_time;
} qos_bucket_t;

/* QOS client specific struture for accounting */
typedef struct QoS_perClient_Class {
	/* Struction belongs to which gsh_client session,
	 * and will be valid till OP_DESTROY/final session deletion
	 **/
	sockaddr_t *client_addr;
	/* Used in case of only PSPC for share->clients->next  */
	struct QoS_perClient_Class *next;
	/*  Used for waitying IO accounting in case of token exhaust */
	unsigned int num_ios_waiting;
	bool bw_enabled;
	bool token_enabled;
	bool combined_rw_bw_control;
	bool combined_rw_token_control;
	pthread_mutex_t lock;
	struct qos_bucket read_bucket;
	struct qos_bucket write_bucket;
	/* Struture used for saving IO's after token exuast */
	struct qos_client_entry *client_entries;
} qos_client_t;

/* QOS share specific struture for accounting */
typedef struct QoS_perShare_Class {
	unsigned int share_id;
	/*  Used for waitying IO accounting in case of token exhaust */
	unsigned int num_ios_waiting;
	bool bw_enabled;
	bool token_enabled;
	bool combined_rw_bw_control;
	bool combined_rw_token_control;
	/* lock used for adding/removing clients, client_entries etc */
	pthread_mutex_t lock;
	struct qos_bucket read_bucket;
	struct qos_bucket write_bucket;
	struct QoS_perClient_Class *clients;
	uint64_t max_client_wbw;
	uint64_t max_client_rbw;
	/* Entry is used to store the IO's after token exausted */
	struct qos_client_entry *client_entries;
} qos_share_t;

typedef struct qos_block_config {
	bool enable_qos;
	bool enable_tokens;
	bool enable_bw_control;
	bool combined_rw_bw_control;
	bool combined_rw_token_control;
	int qos_type;
	uint64_t max_export_write_bw;
	uint64_t max_export_read_bw;
	uint64_t max_client_write_bw;
	uint64_t max_client_read_bw;

	uint64_t max_export_read_tokens;
	uint64_t max_export_write_tokens;
	uint64_t max_client_read_tokens;
	uint64_t max_client_write_tokens;
	uint64_t export_read_tokens_renew_time;
	uint64_t export_write_tokens_renew_time;
	uint64_t client_read_tokens_renew_time;
	uint64_t client_write_tokens_renew_time;
} qos_block_config_t;

extern qos_block_config_t qos_block_config;
extern struct config_block qos_core;
/* Structured for Future Generic Class implementation
struct Qos_Class
{
   enum qos_entity_type type;
   union  {
	sockaddr_t *client_addr
	uint64_t shareid;
   }
   struct Qos_Class *next;
   pthread_mutex_t lock;
   unsigned int num_ios_waiting;
   struct qos_bucket read_bucket;
   struct qos_bucket write_bucket;
   struct Qos_Class *subclass;
   struct qos_client_entry *t_exhausted_client;
   void *private;
}
*/

void QoS_perShareInsert(struct gsh_export *export,
			struct qos_block_config *qos_block);
void qos_free_mem(void *gsh_ptr, unsigned int qos_class_type);
void qos_drain_bw_ios(void *qos_class, unsigned int qos_class_type);
unsigned int QoS_Process(unsigned int size, void *caller_data,
			 compound_data_t *data, unsigned int op_type);
qos_client_t *pspc_get_client_from_list(qos_client_t *head,
					sockaddr_t *client_addr);
