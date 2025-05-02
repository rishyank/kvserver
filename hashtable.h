#pragma once

#include <stddef.h>
#include <stdint.h>
#include <cstdio>

// hashtable node, should be embedded into the payload
struct HNode{
    uint64_t hcode = 0;
    HNode *next = NULL;
};

// define hashtable
struct HTab{
    HNode **tab;   // array of HNodes
    size_t mask =0;
    size_t size = 0;
};

// the real hashtable interface.
// it uses 2 hashtables for progressive resizing.
struct HMap
{
    HTab ht1;
    HTab ht2;
    size_t resizing_pos = 0;
};





void hm_insert(HMap* hmap, HNode* node);  // to insert in hmap it is needed to have the map, the entry
HNode* hm_lookup(HMap* hmap, HNode* key,bool(*eq)(HNode *, HNode *));
HNode *hm_pop(HMap *hmap, HNode *key, bool(*eq)(HNode *, HNode *));
size_t hm_size(HMap *hmap);
void hm_destroy(HMap *hmap);
