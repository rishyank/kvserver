#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include "zset.h"
#include "common.h"
#include <iostream>



static ZNode *znode_new(const char *name, size_t len, double score) {
    ZNode *node = (ZNode *)malloc(sizeof(ZNode) + len);
    assert(node);   // not a good idea in real projects
    avl_init(&node->tree);
    node->hnode.next = NULL;
    node->hnode.hcode = str_hash((uint8_t *)name, len);
    node->score = score;
    node->len = len;
    memcpy(&node->name[0], name, len);
    return node;
}

// a helper structure for the hashtable lookup
struct HKey {
    HNode node;
    const char *name = NULL;
    size_t len = 0;
};

static bool hcmp(HNode *node, HNode *key) {
    ZNode *znode = container_of(node, ZNode, hnode);
    HKey *hkey = container_of(key, HKey, node);
    if (znode->len != hkey->len) {
        return false;
    }
    return 0 == memcmp(znode->name, hkey->name, znode->len);
}

// lookup by name
ZNode *zset_lookup(ZSet *zset, const char *name, size_t len) {
    if (!zset->tree) {
        return NULL;
    }

    HKey key;
    key.node.hcode = str_hash((uint8_t *)name, len);
    key.name = name;
    key.len = len;
    HNode *found = hm_lookup(&zset->hmap, &key.node, &hcmp);
    return found ? container_of(found, ZNode, hnode) : NULL;
}

// add a new (score, name) tuple, or update the score of the existing tuple
bool zset_add(ZSet *zset, const char *name, size_t len, double score) {
    ZNode *node = zset_lookup(zset, name, len);
    if (node) {
        zset->tree = avl_delete(zset->tree, node);
        node->score = score;
        avl_init(&node->tree);
        zset->tree = avl_insert(zset->tree, node);
        return false; // Node was updated, not newly added
    } else {
        node = znode_new(name, len, score);
        hm_insert(&zset->hmap, &node->hnode);
        zset->tree = avl_insert(zset->tree, node);
        return true;
    }
}

ZNode *zset_query(ZSet *zset, double score, const char *name, size_t len) {
    AVLNode* found = nullptr; // To store the candidate node

    AVLNode* root = zset->tree;
    while (root){
        ZNode* rootData = container_of(root, ZNode, tree);

        // Compare the current node with the target (score, name)
        if (rootData->score < score ||
            (rootData->score == score && rootData->len < len) ||
            (rootData->score == score && rootData->len == len && strcmp(rootData->name, name) < 0)) {
            // Current node is less than the target, go to the right subtree
            root = root->right;
        } else {
            // Current node is a valid candidate, go to the left subtree
            found = root;
            root = root->left;
        }
    }

    return found ? container_of(found, ZNode, tree) : NULL; // Return the candidate node (or nullptr if none found)
}

// offset into the succeeding or preceding node.
ZNode *znode_offset(ZNode *node, int64_t offset) {
    AVLNode *tnode = node ? avl_offset(&node->tree, offset) : NULL;
    return tnode ? container_of(tnode, ZNode, tree) : NULL;
}

ZNode *zset_pop(ZSet *zset, const char *name, size_t len) {
    if (!zset->tree) {
        return NULL;
    }

    HKey key;
    key.node.hcode = str_hash((uint8_t *)name, len);
    key.name = name;
    key.len = len;
    HNode *found = hm_pop(&zset->hmap, &key.node, &hcmp);
    if (!found) {
        return NULL;
    }

    ZNode *node = container_of(found, ZNode, hnode);
    zset->tree = avl_delete(zset->tree,node);
    return node;
}

void tree_dispose(AVLNode* root) {
    if (!root) {
        return;
    }

    tree_dispose(root->left);  // Delete left subtree
    tree_dispose(root->right); // Delete right subtree

    ZNode* nodeData = container_of(root, ZNode, tree);
    free(nodeData); // Free the ZNode memory
}

void zset_dispose(ZSet *zset) {
    tree_dispose(zset->tree);
    hm_destroy(&zset->hmap);
}

void znode_del(ZNode *node) {
    free(node);
}

