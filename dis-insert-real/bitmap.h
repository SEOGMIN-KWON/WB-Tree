#ifndef BITMAP_H
#define BITMAP_H
#include <cstring>
#include <iostream>

#define __const_hweight8(w)		\
	((unsigned int)			\
	 ((!!((w) & (1ULL << 0))) +	\
	  (!!((w) & (1ULL << 1))) +	\
	  (!!((w) & (1ULL << 2))) +	\
	  (!!((w) & (1ULL << 3))) +	\
	  (!!((w) & (1ULL << 4))) +	\
	  (!!((w) & (1ULL << 5))) +	\
	  (!!((w) & (1ULL << 6))) +	\
	  (!!((w) & (1ULL << 7)))))

#define __const_hweight16(w) (__const_hweight8(w)  + __const_hweight8((w)  >> 8 ))
#define __const_hweight32(w) (__const_hweight16(w) + __const_hweight16((w) >> 16))
#define __const_hweight64(w) (__const_hweight32(w) + __const_hweight32((w) >> 32))

#define BITS_PER_BYTE 8
#define BITS_PER_LONG 64
#define _BITOPS_LONG_SHIFT 6
#define DIV_ROUND_UP(n,d) (((n) + (d) -1) / (d))
#define BITS_TO_LONGS(nr) DIV_ROUND_UP(nr, BITS_PER_BYTE * sizeof(long))

#define DECLARE_BITMAP(name, bits) \
	unsigned long name[BITS_TO_LONGS(bits)]

#define BITOP_ADDR(x) "+m" (*(volatile long *) (x))
#define ADDR	BITOP_ADDR(addr)


static inline void __set_bit(long nr, volatile unsigned long *addr)
{
	asm volatile("bts %1,%0": ADDR : "Ir" (nr) : "memory");
}

static inline void __clear_bit(long nr, volatile unsigned long *addr)
{
	asm volatile("btr %1,%0" : ADDR : "Ir" (nr));
}

static inline bool const_test_bit(long nr, const volatile unsigned long *addr)
{
	return ((1UL << (nr & (BITS_PER_LONG-1))) & (addr[nr >> _BITOPS_LONG_SHIFT])) != 0;
}

/*
 *__ffs - find first set bit in word
 * */
static inline unsigned long __ffs(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "rm" (word));
	return word;
}

/*
 *__fls: find last set bit in word
 * */
static inline unsigned long __fls(unsigned long word)
{
	asm("bsr %1,%0"
		: "=r" (word)
		: "rm" (word));
	return word;
}
/*
 *ffz- find first zero bit in word
 * */
static inline unsigned long __ffz(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "r" (~word));
	return word;
}

static inline unsigned long hweight_long(unsigned long w)
{
	return sizeof(w) == 4 ? __const_hweight32(w) : __const_hweight64(w);
}

#if 0
static inline bool variable_test_bit(long, volatile const unsigned long *addr)
{
	bool oldbit;
	asm volatile("bt %2,%1\n\t"
			CC_SET(c)
			:CC_OUT(c) (oldbit)
			: "m" (*(unsigned long *)addr), "Ir" (nr));
	return oldbit;
}
#endif













#endif
