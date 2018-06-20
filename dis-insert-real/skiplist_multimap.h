#ifndef STX_STX_SKIPLIST_MULTIMAP_H_HEADER
#define STX_STX_SKIPLIST_MULTIMAP_H_HEADER

#include "skiplist.h" 

namespace stx{
template <typename _Key, typename _Value,
		 typename _Compare = std::less<_Key>,
		 typename _Traits = skiplist_default_set_traits<_Key>,
		 typename _Alloc = std::allocator<_Key> >
class skiplist_multimap
{
public:
	typedef _Key key_type;
	typedef _Value value_type;
	typedef _Compare key_compare;
	typedef _Traits traits;
	typedef _Alloc allocator_type;
private:
	struct empty_struct
	{};
public:
	typedef struct empty_struct empty_type;

	typedef std::pair<key_type, value_type> data_type;

	typedef skiplist_multimap<key_type, value_type, key_compare, traits, allocator_type> self_type;

	typedef stx::skiplist<key_type, value_type, traits> skiplist_impl;

	typedef typename skiplist_impl::sl_iter iterator;

	typedef typename skiplist_impl::skiplist_t sl;
	sl * slist;

private:
		skiplist_impl list;
public:
		skiplist_multimap()
	{
		slist = list.sl_alloc(NULL);
	}

	~skiplist_multimap()
	{
#ifdef PROFILING
		cout << " nventry_count "<<list.nventry_count << " nvbitmap_count " << list.nvbitmap_count << " nvnode_count " << list.nvnode_count << " split_count " << list.split_count<<endl;

		cout << " level 10 " << list.level10_hit << endl;
		cout << " level 9 " << list.level9_hit << endl;
		cout << " level 8 " << list.level8_hit << endl;
		cout << " level 7 " << list.level7_hit << endl;
		cout << " level 6 " << list.level6_hit << endl;
		cout << " level 5 " << list.level5_hit << endl;
		cout << " level 4 " << list.level4_hit << endl;
		cout << " level 3 " << list.level3_hit << endl;
		cout << " level 2 " << list.level2_hit << endl;
		cout << " level 1 " << list.level1_hit << endl;
		cout << " level 0 " << list.level0_hit << endl;
#endif
		int leaf_count = list.sl_free(slist);
	//	cout << "there are " << leaf_count << " leafs " << endl;
		list.sl_destroy(slist);
	}

public:
		inline value_type insert(const key_type &key, const value_type &value)
		{
			return list.sl_insert(slist, key, value);
		}

		inline value_type insert(const data_type& x)
		{
			return insert(x.first, x.second);
		}

		inline value_type find(const key_type &key)
		{
			return list.sl_lookup(slist, key);
		}
		
		inline void reorgnization()
		{
		//	list.print_height(slist);
		//	list.reorgnization(slist);
		}

};

}

#endif
