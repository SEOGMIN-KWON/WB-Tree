#ifndef STX_SKIPLIST_HEADER
#define STX_SKIPLIST_HEADER
#include <algorithm>
#include <functional>
#include <istream>
#include <ostream>
#include <memory>
#include <stdlib.h>
#include <assert.h>
#include <immintrin.h>
#include <bitset>
#include <climits>
#include <set>
#include <unordered_map>
#include "nv_backend.h"
#include "bitmap.h"
#include <libvmem.h>
#include <libpmem.h>
#define SPAN_TH 3
#define MAX_LEVELS 8 
//#define HEAD_COUNT 40
//#define HASH_KEY 55000000
#define HEAD_COUNT 400
#define HASH_KEY 5500000

//#define DEBUGS

#define EXPECT_TRUE(x)	__builtin_expect(!!(x), 1)
#define EXPECT_FALSE(x)	__builtin_expect(!!(x), 0)

#define CAS_EXPECT_DOES_NOT_EXIST (0)
#define CAS_EXPECT_EXISTS		(-1)
#define	CAS_EXPECT_WHATEVER		(-2)
#define SYNC_SWAP(addr, x)		__sync_lock_test_and_set(addr, x)
#define SYNC_CAS(addr, old, x)	__sync_val_compare_and_swap(addr, old, x)
#define SYNC_ADD(addr, n)		__sync_add_and_fetch(addr, n)
#define SYNC_FETCH_AND_OR		__sync_fetch_and_or(addr, x)


#define TAG_VALUE(v, tag) ((v) | tag)
#define IS_TAGGED(v, tag) ((v) & tag)
#define STRIP_TAG(v, tag) ((v) & ~tag)

#define PMEM_LEN 1.999 * 1024 * 1024 * 1024

using namespace std;
typedef size_t markable_t;

//Marking the <next> field of a node logically removes it from the list
#define MARK_NODE(x) TAG_VALUE(reinterpret_cast<markable_t>(x), 0x1)
#define HAS_MARK(x) (IS_TAGGED((x), 0x1) == 0x1) 
#define GET_NODE(x) (reinterpret_cast<dnode_t *>(x))
#define STRIP_MARK(x) (reinterpret_cast<dnode_t *>(STRIP_TAG((x), 0x1)))

enum unlink {
	FORCE_UNLINK,
	ASSIST_UNLINK,
	DONT_UNLINK
};

namespace stx {
	template <typename _Key>
		class skiplist_default_set_traits
		{
			static const bool selfverify = false;
			static const bool debug = false;
			static const int leafslots = 256;
		};

	template <typename _Key,  typename _Value, typename _Traits = skiplist_default_set_traits<_Key> >	
		class skiplist
		{
			public:

				typedef _Key key_type;

				typedef _Value value_type;

				typedef _Traits traits;

				typedef struct datatype {
				}datatype_t;

			public:

				/// Size type usedjkj to count keys
				typedef size_t size_type;

				typedef std::pair<key_type, value_type> pair_type;
			public:

				///Base Skiplist parameter: The numener of key/value slots in each leaf
				static const unsigned long leafslotmax = traits::leafslots;

			private:
				static const int rs = 12345678;

			public:
				typedef struct kv_entry
				{
					key_type key;
					value_type value;
				}entry_t;

				typedef struct nvram_node {
					unsigned long bs[BITS_TO_LONGS(leafslotmax)] ;
					entry_t slotentry[leafslotmax];
					struct nvram_node *next;

					void reset()
					{
						memset(bs, 0x0, BITS_TO_LONGS(leafslotmax) * sizeof(unsigned long));
					}

					inline long count() const
					{
						return hweight_long(*bs);
					}

					///True if the node's slots are full
					inline bool isfull() const
					{
						return (count() == leafslotmax) ? true : false;
					}

					///set the indexth bit to 1 
					inline void bitmap_set(long index)
					{
						__set_bit(index, bs);
					}

					///return index th bit's value
					inline int bitmap_get(long index)
					{
						return const_test_bit(index, bs);
					}

					///set the index th bit to 0 
					inline void bitmap_clear(long index) 
					{
						__clear_bit(index, bs);
					}

					///return the first bit that was set to 1, from left to right
					inline long get_first_set_bit()
					{
						return __ffs(*bs);
					}

					///return last set bit in word
					inline long get_last_set_bit()
					{
						return __fls(*bs);
					}
					

					///return the first bit that was 0. from left to right
					inline long get_first_free_bit()
					{
						return __ffz(*bs);
					}

					inline int set(key_type key, value_type value) {
						int index = get_first_free_bit(); 			
						slotentry[index].key = key;
						slotentry[index].value = value;
						return index;
					}

					value_type get(key_type key) {
						int leafslotsused = count();
						for(int i = leafslotsused - 1; i >= 0; i--) {
							if(bitmap_get(i)){
								if(slotentry[i].key == key){
									return slotentry[i].value;
								//	return i;
								}
							}
						}
						return -1;
					}

				}nvnode_t;

				typedef struct dram_node {
					key_type sum; //sum of all the key in the leaf node
					key_type min; //minimum key in its data node
					key_type max;  //max key in its data node
					bool is_head;
					unsigned num_levels;
					nvnode_t *nv_node;
					struct dram_node *next[MAX_LEVELS];
				} dnode_t;

				typedef struct sl {
					dnode_t *heads[HEAD_COUNT];
					const datatype_t *type;
					int current_levels[HEAD_COUNT];
					char *dnodes_pool;
					VMEM *vmp;
				} skiplist_t ;

				struct sl_iter {
					dnode_t *next;
				};

			public:
				nvnode_t *nvnode_alloc(skiplist_t *sl){
					nvnode_t *item = reinterpret_cast<nvnode_t *>(nv_malloc(sizeof(nvnode_t), sl->vmp));
					item->reset();
					item->next = NULL;
					return item;
				}

				int nvnode_flush(nvnode_t *nv_node)
				{
					size_t sz = sizeof(nvnode_t);
					nv_flush((char *)nv_node, sz);
					return 0;
				}

				void dnode_init(dnode_t *item, int num_levels, key_type max = 0, key_type min = ULLONG_MAX, key_type sum = 0, nvnode_t *leaf= NULL){

					assert(num_levels >= 0 && num_levels <= MAX_LEVELS);
					item->max = max;
					if(min == ULLONG_MAX)
						item->min = max;
					else
						item->min = min;
					item->num_levels = num_levels;
					item->sum = sum;
					item->nv_node = leaf;
				}

				void dnodes_poll_alloc(skiplist_t *sl)
				{
					sl->dnodes_pool = reinterpret_cast<char *>(calloc(1, 800000 * sizeof(dnode_t)));
				}

				dnode_t *get_dnode(skiplist_t *sl) 
				{
					dnode_t *inode = reinterpret_cast<dnode_t *>(sl->dnodes_pool);
					sl->dnodes_pool += sizeof(*inode);
					return inode;
				}

				skiplist_t *sl_alloc (const datatype_t *type) {
					skiplist_t *sl = static_cast<skiplist_t *>(malloc(sizeof(skiplist_t)));
					sl->type = type;
					sl->vmp = nv_create("/mnt/nvram0", PMEM_LEN);
					if(sl->vmp == NULL){
						cout << " nv_create failed " << endl;
					}
					dnodes_poll_alloc(sl);
					int i; 
					for(i = 0; i < HEAD_COUNT; i++){
						nvnode_t *leaf = nvnode_alloc(sl);
						sl->heads[i] = get_dnode(sl);
						sl->current_levels[i] = 1;
						dnode_init(sl->heads[i], MAX_LEVELS, 0, 0, 0, leaf);
						memset(sl->heads[i]->next, 0x0, MAX_LEVELS * sizeof(dnode_t *));
						sl->heads[i]->is_head = true;
						if(i > 0) {
							for(int j = 0; j< MAX_LEVELS; j++) {
								sl->heads[i - 1]->next[j] = sl->heads[i];		
							}
							sl->heads[i-1]->nv_node->next = sl->heads[i]->nv_node;
							nvnode_flush(sl->heads[i-1]->nv_node);	
						}
					}
					return sl;
				}

				void sl_destroy(skiplist_t *sl)
				{
					nv_destroy(sl->vmp);
				}

			    nvnode_t *split_leaf_node(skiplist_t *sl, key_type *max_key, key_type *min_key, 
						const key_type target_key, nvnode_t *old_nvnode, nvnode_t *orig_nvnode, 
						key_type *new_sum, key_type *orig_sum) {
					nvnode_t *new_nvnode = nvnode_alloc(sl);
					for(int index = old_nvnode->count() - 1; index >= 0; index--){
						if(old_nvnode->bitmap_get(index) != 1)
							continue;
						key_type temp_key = old_nvnode->slotentry[index].key;
						key_type temp_value = old_nvnode->slotentry[index].value;
						if(temp_key <= target_key ) {
							int ix = new_nvnode->set(temp_key, temp_value);
							new_nvnode->bitmap_set(ix);
							if(temp_key >= *max_key){
								*max_key = temp_key;
							}
							*new_sum += temp_key;
						}else{
							int ix = orig_nvnode->set(temp_key, temp_value);
							orig_nvnode->bitmap_set(ix);
							if(temp_key <= *min_key){
								*min_key = temp_key;
							}
							*orig_sum += temp_key;
						}
					}
					return new_nvnode;
				}

				inline key_type get_split_key(dnode_t *index_node)
				{
					key_type target;
					target = (index_node->sum - index_node->min - index_node->max) / (leafslotmax - 2);
					if(target > index_node->max) {
						cout << " hoho " << endl;	
					}
					return target;
				}

				int sl_free (skiplist_t *sl) {
					size_t count = 0;
					dnode_t *item = sl->heads[0]->next[0];
					while(item) {
						nv_free(sl->vmp, (char *)(item->nv_node));
						count++;
						item = item->next[0];
					}
					return count;
				}

				//find the index node of certain key. n is the random level of the new node
				dnode_t *find_index_node_lookup(skiplist_t *sl, key_type key) {
					int no_head = key / HASH_KEY; 
					dnode_t *pred = sl->heads[no_head];
					dnode_t * item = NULL; //item is the pointer point to target index node.
					for(int level = sl->current_levels[no_head] - 1; level >= 0; --level) {
						dnode_t *item = pred->next[level];
						while (item != NULL && item->is_head != true) {
							if(key > item->max) {
								pred = item;
								item = item->next[level];
							}else if(key < item->min) {
								break;
							}
							else {
								return item;
							}
						} // end of while;
					} //end of for 
					return NULL;
				}

				dnode_t  *find_inode_build(dnode_t **preds, bool *is_full, skiplist_t *sl, key_type key) {
					int no_head = key / HASH_KEY; 
					dnode_t *pred = sl->heads[no_head];
					dnode_t *item = NULL;
					for(int level = sl->current_levels[no_head] - 1; level >= 0; --level) {
						dnode_t *starter = pred;
						item = pred->next[level];
						int span = 0;
						while(item != NULL && item->is_head != true) {
							if(key > item->max) {
								if(span >= SPAN_TH && level == item->num_levels - 1 && level < MAX_LEVELS - 1) {
										item->num_levels += 1;
										if(item->num_levels > MAX_LEVELS){
											item->num_levels = MAX_LEVELS;
										}
										if(item->num_levels > sl->current_levels[no_head]){
											sl->current_levels[no_head] = item->num_levels;
										}
										item->next[level + 1] = starter->next[level + 1];
										starter->next[level + 1] = item; 
										span = 0;
								}
								pred = item;
								item = item->next[level];
								span++;
							}else if(key < item->min) {
								if(span == 0 && starter->next[level + 1] == item && level == item->num_levels - 2) {
									item->num_levels -= 1;
									starter->next[level + 1] = item->next[level + 1];
									item->next[level + 1] = NULL;
								}
								break;
							}else {
								if(!item->nv_node->isfull()) {
									return item;
								}else {
									break;
								};
							}
						} // end of while
						if(preds != NULL)
							preds[level] = pred;
					} // end of for
					if(preds[0]->next[0] && preds[0]->next[0]->is_head != true){
						item = preds[0]->next[0];
						if(item->nv_node->isfull()) {
							*is_full = true;
						}
						return item;
					}else {
						item = preds[0];
						if(item == sl->heads[no_head]){
							return NULL;
						}
						else if(item->nv_node->isfull()) {
							return NULL; 
						}
						else {
							return item;
						}
					}
				}

				value_type sl_lookup(skiplist_t *sl, key_type key) {
					dnode_t *inode = NULL;	
					inode = find_index_node_lookup(sl, key);
					return 0;
					nvnode_t *nv_node = inode->nv_node;
					value_type value = nv_node->get(key);
						return value;
				}
#if 0
				key_type sl_min_key (skiplist_t *sl) {
					dnode_t *item = GET_NODE(sl->head->next[0]);
					while(item != NULL) {
						markable_t next = item->next[0];
						if(!HAS_MARK(next))
							return item->max;
						item = STRIP_MARK(next);
					}
					return NULL;
				}
#endif
				void print_height(skiplist_t *sl)
				{
					dnode_t *pred = sl->head;
					dnode_t *item = NULL;
					item = pred->next[0];
					while(item != NULL)
					{
						cout << item->num_levels << endl;
						item = item->next[0];
					}
				}

				value_type sl_insert(skiplist_t *sl, key_type new_key, value_type new_val, nvnode_t *nv_node = NULL)
				{
					dnode_t *preds[MAX_LEVELS];
					bool is_full = false;
/*
* find the indexing dram node
*/
					dnode_t * inode = find_inode_build(preds, &is_full, sl, new_key);

/* 
 * index_nodes exists: key belongs to (min, max);
 * nexts[0] exists: index nodes exists, however, key < min; 
 * anyway, nexts[0] == index_node; insert the k,v pair 
 * to its leaf node
 */
					if (inode) {
						nv_node = inode->nv_node; // read from the nvram 

						if(!is_full) {
							int ix = nv_node->set(new_key, new_val); 
							nv_flush((char *)(nv_node->slotentry+ix), sizeof(entry_t));
							int bitmap_sz = BITS_TO_LONGS(leafslotmax) * sizeof(unsigned long);
#if 0
							unsigned long *shadow_bs = reinterpret_cast<unsigned long *>(nv_malloc(bitmap_sz, sl->vmp));
							memcpy(shadow_bs, nv_node->bs, bitmap_sz);
							nv_flush((char *)shadow_bs, bitmap_sz);
#endif
							nv_node->bitmap_set(ix);
							nv_flush((char *)&(nv_node->bs), bitmap_sz);
							//nv_free(sl->vmp, (char *)shadow_bs);
/*
 * update index_node
 */
							if(new_key < inode->min)
								inode->min = new_key;
							else if(new_key > inode->max)
								inode->max = new_key;
							inode->sum += new_key;
						}else { //need to split
							key_type min_key = ULLONG_MAX; // new min key of original leaf node  
							key_type max_key = 0;// new max key of new leaf node
							key_type orig_sum = 0; //sum of each key in original's leaf after split.
							key_type new_sum = 0; //sum of each key in new;s leaf after split.
							key_type target_key = 0;

							if(inode->min == inode->max ) {
								nvnode_t *orig_nvnode = nvnode_alloc(sl);
								int ix = orig_nvnode->set(nv_node->slotentry[leafslotmax -1].key, nv_node->slotentry[leafslotmax - 1].value);
								orig_nvnode->bitmap_set(ix);
								ix = orig_nvnode->set(new_key, new_val);
								orig_nvnode->bitmap_set(ix);
								orig_nvnode->next = nv_node->next;
								nvnode_flush(orig_nvnode);
								SYNC_CAS(&preds[0]->nv_node->next, nv_node, orig_nvnode);
								nv_free(sl->vmp, (char *)nv_node);
								if(new_key < inode->min)
									inode->min = new_key;
								else if(new_key > inode->max)
									inode->max = new_key;
								inode->nv_node = orig_nvnode;
							}else {
								target_key = get_split_key(inode);
								nvnode_t *orig_nvnode = nvnode_alloc(sl); 
								nvnode_t *new_nvnode = split_leaf_node(sl, &max_key, &min_key, target_key, nv_node, orig_nvnode, &new_sum, &orig_sum); 
/* 
 * get two new leafs, each contains half of entries of nv_node;
   insert the new key,value into either orignal node or new node
*/
								if(new_key < min_key) {
									if(new_key >= max_key)
										max_key = new_key;
									int ix = new_nvnode->set(new_key, new_val); 
									new_nvnode->bitmap_set(ix);
									new_sum += new_key;
								}else {
									int ix = orig_nvnode->set(new_key, new_val);
									orig_nvnode->bitmap_set(ix);
									orig_sum += new_key;
								}	 
/*
 * link two new nv_nodes
 */
								orig_nvnode->next = nv_node->next;
								new_nvnode->next = orig_nvnode;
/*
 * link completed
 */
								nvnode_flush(orig_nvnode);
								nvnode_flush(new_nvnode);

								SYNC_CAS(&preds[0]->nv_node->next, nv_node, new_nvnode);
								nv_free(sl->vmp, (char *)nv_node);

								//insert completed
								//update dram indexing node
								dnode_t *new_inode = get_dnode(sl);
								dnode_init(new_inode, 1, max_key, min(inode->min, new_key), new_sum, new_nvnode);
								inode->min = min_key;
								inode->sum = orig_sum;
								for(unsigned int level = 0; level < new_inode->num_levels; level++){
									dnode_t *pred = preds[level];
									new_inode->next[level] = pred->next[level];
									pred->next[level] = new_inode;
								}

								inode->nv_node = orig_nvnode;
								//update completed
							}
						}

/* 
 * naither index_node nor nexts[0] exists. 
 * add a total new index node with new leaf_node
 */
					}else { 
						assert(preds[0]);
						nvnode_t *nv_node = nvnode_alloc(sl);
						dnode_t *new_inode = get_dnode(sl);
						dnode_init(new_inode, 1, new_key, ULLONG_MAX, 0, nv_node);
						if(!nv_node->isfull()) {
							int ix = nv_node->set(new_key, new_val); 
							nv_flush((char *)(nv_node->slotentry+ix), sizeof(entry_t));
							int bitmap_sz = BITS_TO_LONGS(leafslotmax) * sizeof(unsigned long);
							nv_node->bitmap_set(ix);
							nv_flush((char *)&(nv_node->bs), bitmap_sz);
							new_inode->sum += new_key;
						}

						dnode_t *pred = preds[0];
						SYNC_CAS(&pred->nv_node->next, 0, nv_node);

						for(unsigned int level = 0; level < new_inode->num_levels; ++level) {
							dnode_t *pred = preds[level];
							new_inode->next[level] = pred->next[level];
							pred->next[level] = new_inode;
						}
						new_inode->nv_node = nv_node;
					}
					return 0;
				}


				// Mark <item > at each level of <sl> from the top down. If multiple threads try to concurrently remove 
				// the same item only one of them should succeed. Marking the bottom level established which of them succe
				// ed.
#if 0
				value_type sl_remove (skiplist_t *sl, key_type key) {
#ifdef DEBUG
					cout << "s1 sl_remove: removing item with key " << key << " from skiplist " << sl << endl;
#endif
					dnode_t *preds[MAX_LEVELS];
					dnode_t *item = find_preds_index(preds, NULL, sl->high_water, sl, key, ASSIST_UNLINK);
					if(item == NULL) {
#ifdef DEBUG
						cout << "s3 sl_remove: remove failed, an item with a matching key does not exist in the skiplist" << endl;
#endif
						return NULL;
					}

					// Mark <item> at each level of <sl> from the top down. if multiple threads try to concurrently remove
					// the same item only one of them should succeed. Marking the bottom level establishes which of 
					// them succeeds.
					markable_t old_next = 0;
					for (int level = item->num_levels - 1; level >= 0; --level) {
						markable_t next;
						old_next = item->next[level];
						do {
#ifdef DEBUG
							cout << "s3 sl_remove: marking item at level " << level << "next " << old_next << endl;
#endif
							next = old_next;
							old_next = SYNC_CAS(&item->next[level], next, MARK_NODE(static_cast<dnode_t *>(next)));
							if(HAS_MARK(old_next)) {
#ifdef DEBUG
								cout << " s2 sl_remove: " << item << " is already marked for remove by another thread next " << old_next << endl;
#endif
								if(level == 0)
									return NULL;
								break;
							}
						}while(next != old_next);
					}

					//Atomically swap out the item's value in case another thread is updating the item while we are 
					//removing it. This establishes which operation occurs first logically, the update or the remove.
					value_type val = SYNC_SWAP(&item->min, NULL);
#ifdef DEBUG
					cout << " sw sl_remove: replaced item " << item << " 's value whit NULL " << endl;
#endif
					//unlink the item
					find_preds_index(NULL, NULL, 0, sl, key, FORCE_UNLINK);

					//free the node
					if(sl->type !=NULL) {
						free(static_cast<void *>(item->max));	
					}
					free(item);

					return val;
				}
#endif
				sl_iter * sl_iter_begin(skiplist_t *sl, key_type key) {
					sl_iter *iter = static_cast<sl_iter *>(malloc(sizeof(sl_iter)));
					if(key != NULL) {
						find_preds_index(NULL, &iter->next, 1, sl, key ,DONT_UNLINK);
					}else {
						iter->next = GET_NODE(sl->head->next[0]);
					}
					return iter;
				}

				value_type sl_iter_next(sl_iter *iter, key_type * key_ptr) {
					dnode_t *item = iter->next;
					while ( item != NULL && HAS_MARK(item->next[0])) {
						item = STRIP_MARK(item->next[0]);
					}
					if(item == NULL) {
						iter->next = NULL;
						return NULL;
					}

					iter->next = STRIP_MARK(item->next[0]);
					if ( key_ptr != NULL) {
						*key_ptr = item->max;
					}
					return item->min;
				}

				void sl_iter_free ( sl_iter *iter) {
					free(iter);
				}

		};
};
#endif
