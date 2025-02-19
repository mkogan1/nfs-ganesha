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

#include "nfsService.grpc.pb.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/param.h>
#include <pthread.h>

extern "C" {
#include "nfs_core.h"
#include "log.h"
#include "sal_data.h"
#include "sal_functions.h"
#include "config.h"
}

class GetClientIdService final : public nfsService::GetClientId::Service {
public:
    grpc::Status GetClientIds(grpc::ServerContext* context,
                          const nfsService::GetClientIdsRequest* request,
                          nfsService::GetClientIdsResponse* response) override {

        	 std::vector<uint64_t> client_ids;
		 hash_table_t *ht;
		 ht = ht_confirmed_client_id;
		 nfs_client_id_t *pclientid;
        	 struct hash_data *pdata = NULL;

		 for (uint32_t i = 0; i < ht->parameter.index_size; ++i) {
            	 	struct rbt_head* head_rbt = &(ht->partitions[i].rbt);
            
            		PTHREAD_RWLOCK_wrlock(&(ht->partitions[i].ht_lock));
            		struct rbt_node* pn;
            		RBT_LOOP(head_rbt, pn) {
                		pdata = (hash_data*)RBT_OPAQ(pn);
                		pclientid = (nfs_client_id_t*)pdata->val.addr;
                		uint64_t clientid = pclientid->cid_clientid;
                		client_ids.push_back(clientid);  // Add the client ID to the list
                		RBT_INCREMENT(pn);
            		} // RBT_LOOP
	    		PTHREAD_RWLOCK_unlock(&(ht->partitions[i].ht_lock));
        	} // for loop

        	// Add the client IDs to the response
        	for (auto& id : client_ids) {
            	response->add_client_ids(id);  // Adds client ID to the repeated field
        	} // for loop
	
	    	return grpc::Status::OK;
    }
};

class GetNfsGraceService final : public nfsService::GetNfsGrace::Service {
public:
	grpc::Status GetGracePeriod(grpc::ServerContext* context,
				const nfsService::GetNfsGraceRequest* request,
				nfsService::GetNfsGraceResponse* response) override {

			bool ingrace = nfs_in_grace();  // Function to check if in grace period
        
        		// Set the response
        		response->set_ingrace(ingrace);
        
        		return grpc::Status::OK;
		}
};

class GetSessionIdService final: public nfsService::GetSessionId::Service {
	public:
		grpc::Status GetSessionIds(grpc::ServerContext* context,
                                const nfsService::GetSessionIdsRequest* request,
                                nfsService::GetSessionIdsResponse* response) override {

			        uint32_t i;
				hash_table_t* ht = ht_session_id;
  			 	struct rbt_head* head_rbt;
        			struct hash_data* pdata = NULL;
        			struct rbt_node* pn;
        			char* session_id = (char *)alloca(2 * NFS4_SESSIONID_SIZE);
        			nfs41_session_t* session_data;

        			for (i = 0; i < ht->parameter.index_size; i++) {
            				head_rbt = &(ht->partitions[i].rbt);

            				PTHREAD_RWLOCK_wrlock(&(ht->partitions[i].ht_lock));

            				RBT_LOOP(head_rbt, pn) {
                			pdata = (hash_data*)RBT_OPAQ(pn);
                			session_data = (nfs41_session_t*)pdata->val.addr;

                			b64_ntop((unsigned char*)session_data->session_id, NFS4_SESSIONID_SIZE, session_id, (2 * NFS4_SESSIONID_SIZE));

                			response->add_session_ids(session_id);

                			RBT_INCREMENT(pn);
            				}
            			PTHREAD_RWLOCK_unlock(&(ht->partitions[i].ht_lock));
        			}

                        return grpc::Status::OK;
                }

};
