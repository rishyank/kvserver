#pragma once

#include <stddef.h>
#include <stdint.h>
#include "hashtable.h"
#include "common.h"

struct AVLNode {
    uint32_t height = 0;
    uint32_t count = 0;
    AVLNode *left = NULL;
    AVLNode *right = NULL;
    AVLNode *parent = NULL;
};

struct ZNode {
    AVLNode tree;
    HNode hnode;
    double score = 0;
    size_t len = 0;
    char name[0];
};

inline void avl_init(AVLNode* node) {
    node->height = 1;
    node->count = 1;
    node->left = node->right = node->parent = nullptr;
}


AVLNode* avl_insert(AVLNode* root, ZNode* newNode);
AVLNode* avl_delete(AVLNode* root, ZNode* nodeDelete);
AVLNode *avl_offset(AVLNode *node, int64_t offset);