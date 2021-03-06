/*
 * Copyright (C) 2017, Leo Ma <begeekmyfriend@gmail.com>
 */

#ifndef _BPLUS_TREE_H
#define _BPLUS_TREE_H

/* 5 node caches are needed at least for self, left and right sibling, sibling
 * of sibling, parent and node seeking */
#define MIN_CACHE_NUM 5

/* LINUX에서의 container_of macro 함수 응용 */
#define list_entry(ptr, type, member) \
        ((type *)((char *)(ptr) - (size_t)(&((type *)0)->member)))

#define list_first_entry(ptr, type, member) \
	list_entry((ptr)->next, type, member)

#define list_last_entry(ptr, type, member) \
	list_entry((ptr)->prev, type, member)

#define list_for_each(pos, head) \
        for (pos = (head)->next; pos != (head); pos = pos->next)

#define list_for_each_safe(pos, n, head) \
        for (pos = (head)->next, n = pos->next; pos != (head); \
                pos = n, n = pos->next)

typedef int key_t;

struct list_head {
        struct list_head *prev, *next;
};

//초기화시 첫 노드의 prev, next 모두 일단 자기 자신을 가리킴 
static inline void list_init(struct list_head *link)
{
        link->prev = link;
        link->next = link;
}

// link가 추가 하고자 하는 list_head* : funda ADD function  
static inline void
__list_add(struct list_head *link, struct list_head *prev, struct list_head *next)
{
        link->next = next;
        link->prev = prev;
        next->prev = link;
        prev->next = link;
}

// funda DELETE function : prev와 next 사이의 리스트 헤드 제거 
static inline void __list_del(struct list_head *prev, struct list_head *next)
{
        prev->next = next;
        next->prev = prev;
}

// link를 prev와 prev->next 리스트 헤드 사이에 추가
static inline void list_add(struct list_head *link, struct list_head *prev)
{
        __list_add(link, prev, prev->next);
}

// list에 tail로 추가시 : head->prev를 추가하는link의 prev로, head를 추가하는 link의 next로insert
static inline void list_add_tail(struct list_head *link, struct list_head *head)
{
	__list_add(link, head->prev, head);
}

// 헤드 리스트 link를 리스트에서 제거하고, link를 초기화: link->prev,next 모두 link 자신을 가리킴
static inline void list_del(struct list_head *link)
{
        __list_del(link->prev, link->next);
        list_init(link);
}

// 리스트 헤드 list를 이용해 리스트가 비었는지 check : empty면 !0인 값을 return 
static inline int list_empty(const struct list_head *head)
{
	return head->next == head;
}

/* space effieciency를 위해 physically fixed인 아래 structure 사용 안함 */
/*
struct bplus_non_leaf {
        off_t self;
        off_t parent;
        off_t prev;
        off_t next;
        int type;
        int children;
        key_t key[BPLUS_MAX_ORDER - 1];
        off_t sub_ptr[BPLUS_MAX_ORDER];
};

struct bplus_leaf {
        off_t self;
        off_t parent;
        off_t prev;
        off_t next;
        int type;
        int entries;
        key_t key[BPLUS_MAX_ENTRIES];
        long data[BPLUS_MAX_ENTRIES];
};
*/

// off_t 는 long형 integer 
typedef struct bplus_node {
        off_t self;
        off_t parent;
        off_t prev;
        off_t next;
        int type;
        /* If leaf node, it specifies  count of entries,
         * if non-leaf node, it specifies count of children(branches) */
        int children;
} bplus_node;

typedef struct free_block {
        struct list_head link;
        off_t offset;
} free_block;

struct bplus_tree {
        char *caches;
        int used[MIN_CACHE_NUM];
        char filename[1024];
        int fd;
        int level;
        off_t root;
        off_t file_size;
        struct list_head free_blocks;
};

void bplus_tree_dump(struct bplus_tree *tree);
long bplus_tree_get(struct bplus_tree *tree, key_t key);
int bplus_tree_put(struct bplus_tree *tree, key_t key, long data);
long bplus_tree_get_range(struct bplus_tree *tree, key_t key1, key_t key2);
struct bplus_tree *bplus_tree_init(char *filename, int block_size);
void bplus_tree_deinit(struct bplus_tree *tree);
int bplus_open(char *filename);
void bplus_close(int fd);

#endif  /* _BPLUS_TREE_H */
