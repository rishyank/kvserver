#include <cassert>
#include <cstdlib>
#include "hashtable.h"


static void h_init(HTab* htab, size_t n){

    // check n is is a even 
    assert(n > 0 && ((n - 1) & n) == 0); // compare bitwise mutiplication for powers of 2   
    htab->tab = (HNode**) calloc(n,sizeof(HNode*));
    htab->mask = n-1;
    htab->size = 0;
}  


void h_insert(HTab* htab, HNode* node){

    size_t pos = node->hcode & htab->mask;
    HNode *next = htab->tab[pos];
    htab->tab[pos] = node;
    node->next = next;  
    htab->size++;
} 

static void hm_start_resizing(HMap* hmap){
    assert(hmap->ht2.tab == nullptr);

    hmap->ht2 = hmap->ht1; // point all the values from ht1 to ht2
    h_init(&hmap->ht1, (hmap->ht1.mask +1)*2);
    hmap->resizing_pos = 0;

} 

const size_t k_max_load_factor = 8;

void hm_insert(HMap* hmap, HNode* node){ 

    if (!hmap->ht1.tab){
        h_init(&hmap->ht1,4);
    } 
    
    h_insert(&hmap->ht1,node);

    if (!hmap->ht2.tab){
        size_t load_factor = hmap->ht1.size / (hmap->ht1.mask + 1);
        if (load_factor >= k_max_load_factor){
            hm_start_resizing(hmap); // intiate the resizing process 
        } 
    } 
} 


const size_t k_resizing_work = 128;

static HNode* h_detach(HTab* htab,HNode** from){
    HNode *node = *from;
    *from = node->next;
	htab->size--;
	return node;
} 

static void hm_help_resizing(HMap *hmap){
     size_t nwork = 0;

     while (nwork < k_resizing_work && hmap->ht2.size > 0){
        HNode** from = &hmap->ht2.tab[hmap->resizing_pos];
        if (!*from) {
            hmap->resizing_pos++;
            continue;
        }

        h_insert(&hmap->ht1, h_detach(&hmap->ht2, from));
        nwork++;
     }
      if (hmap->ht2.size == 0 && hmap->ht2.tab) {
        // done
        free(hmap->ht2.tab);
        hmap->ht2 = HTab{};
    } 
} 

HNode** h_lookup(HTab* htab,HNode* key, bool(*eq)(HNode *, HNode *)){
    if (!htab->tab) return nullptr;
    size_t pos = key->hcode & htab->mask;
    HNode **from = &htab->tab[pos]; // incoming pointer to the result 
    // the from consists of pointer to pointer

    for (HNode* curr; (curr = *from)!= nullptr; from = &curr->next ){
        if (curr->hcode == key->hcode && eq (curr,key)){
            return from;
        } 
    } 
    return nullptr;
} 

HNode* hm_lookup(HMap* hmap, HNode* key,  bool(*eq)(HNode *, HNode *)){
    hm_help_resizing(hmap);
    HNode** from = h_lookup(&hmap->ht1,key,eq);

    if (!from){
        from = h_lookup(&hmap->ht2,key,eq);
    } 

    return from ? *from : nullptr;
} 

HNode* hm_pop(HMap* hmap, HNode* key,  bool(*eq)(HNode *, HNode *)){
     hm_help_resizing(hmap);
    
    if (HNode **from = h_lookup(&hmap->ht1, key, eq)) {
		return h_detach(&hmap->ht1, from);
	}
	if (HNode **from = h_lookup(&hmap->ht2, key, eq)) {
		return h_detach(&hmap->ht2, from);
	}
	return NULL;
} 

size_t hm_size(HMap *hmap) {
	return hmap->ht1.size + hmap->ht2.size;
}

void hm_destroy(HMap *hmap) {
    free(hmap->ht1.tab);
    free(hmap->ht2.tab);
    *hmap = HMap{};
}