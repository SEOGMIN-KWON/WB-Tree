#ifndef _NV_BACKEND_H
#define _NV_BACKEND_H
#define USING_NVRAM
#include <sys/mman.h>
#include <stdio.h>
#include <stdint.h>
#include <cstring>
#include <libvmem.h>
#include <libpmem.h>


using namespace std;

#define  mb() asm volatile("mfence" ::: "memory")
#define  rmb() asm volatile("lfence" ::: "memory")
#define  wmb() asm volatile("sfence" ::: "memory")
#define _mm_clwb(addr)\
	asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)addr));

VMEM *nv_create(const char *dir, size_t size){
	VMEM *vmp = NULL;
	if((vmp = vmem_create(dir,size)) == NULL) {
		cout << "vmem create error" << endl;
		exit(1);
	}
	return vmp;
}

inline void clflush(volatile void *p) 
{
	asm volatile("clflush (%0)" ::"r"(p));
}

void *nv_malloc(size_t length, VMEM *vmp = NULL) {
#ifdef USING_NVRAM
	void *ptr;
	ptr = vmem_malloc(vmp, length);
	if(ptr == NULL) {
		cout << "vmem_alloc error "	<< endl;
	}
	return ptr;
#else
	return malloc(length);
#endif
}

void nv_flush(char *ptr, size_t size) {
#ifdef USING_NVRAM
	pmem_persist(ptr, size);
#else
	mb();
	clflush(m);
#endif
}

void *nv_memcpy(void *dest, const void *src, size_t num)
{
#ifdef USING_NVRAM
	return memcpy(dest, src, num);
#else
	return memcpy(dest, src, num);
#endif
}

void nv_free(VMEM *vmp, char *ptr)
{
#ifdef USING_NVRAM
	vmem_free(vmp, ptr);
#else
	free(m);
#endif
}

void nv_destroy(VMEM *vmp)
{
	vmem_delete(vmp);
}

#endif
