#include <cstdint>
#include <cstddef> 
#include <iostream>
#include <algorithm>
#include <set>
#include <assert.h>
#include <cstdlib> 
#include <cstring>
#include "avl.h"
#include "hashtable.h"

struct AVLTree {
    AVLNode* root = nullptr;
};

uint32_t avl_height(AVLNode* node) {
    return node ? node->height : 0;
}

uint32_t avl_getcount(AVLNode* node) {
    return node ? node->count : 0;
}

void updateNode(AVLNode* node) {
    if (node) {
        node->height = 1 + std::max(avl_height(node->left), avl_height(node->right));
        node->count = 1 + avl_getcount(node->left) + avl_getcount(node->right);
    }
}

int getBalanceFactor(AVLNode* node) {
    return node ? static_cast<int>(avl_height(node->left) - avl_height(node->right)) : 0;
}

AVLNode* findMin(AVLNode* node) {
    while (node && node->left) {
        node = node->left;
    }
    return node;
}

AVLNode* rotateRight(AVLNode* z) {
    AVLNode* y = z->left;
    AVLNode* T2 = y->right;

    y->right = z;
    z->left = T2;

    if (T2) T2->parent = z;
    y->parent = z->parent;
    z->parent = y;

    updateNode(z);
    updateNode(y);

    return y;
}

AVLNode* rotateLeft(AVLNode* z) {
    AVLNode* y = z->right;
    AVLNode* T2 = y->left;

    y->left = z;
    z->right = T2;

    if (T2) T2->parent = z;
    y->parent = z->parent;
    z->parent = y;

    updateNode(z);
    updateNode(y);

    return y;
}


AVLNode* avl_insert(AVLNode* root, ZNode* newNode) {
    if (!root) {
        return &newNode->tree;
    }

    ZNode* rootData = container_of(root, ZNode, tree);

    // Check if the names match
    if (strcmp(newNode->name, rootData->name) == 0) {
        // Update the score if the names match
        rootData->score = newNode->score;
        return root;
    }

    // Compare based on score, then len, then name
    if (newNode->score < rootData->score ||
        (newNode->score == rootData->score && newNode->len < rootData->len) ||
        (newNode->score == rootData->score && newNode->len == rootData->len && strcmp(newNode->name, rootData->name) < 0)) {
        root->left = avl_insert(root->left, newNode);
        root->left->parent = root;
    } else if (newNode->score > rootData->score ||
               (newNode->score == rootData->score && newNode->len > rootData->len) ||
               (newNode->score == rootData->score && newNode->len == rootData->len &&
                strcmp(newNode->name, rootData->name) > 0)) {

        root->right = avl_insert(root->right, newNode);
        root->right->parent = root;
    }

    updateNode(root);

    int balance = getBalanceFactor(root);

    // Balancing the AVL tree
    if (balance > 1) {
        if (getBalanceFactor(root->left) >= 0) {
            return rotateRight(root);
        } else {
            root->left = rotateLeft(root->left);
            return rotateRight(root);
        }
    }

    if (balance < -1) {
        if (getBalanceFactor(root->right) <= 0) {
            return rotateLeft(root);
        } else {
            root->right = rotateRight(root->right);
            return rotateLeft(root);
        }
    }

    return root;
}


AVLNode* avl_delete(AVLNode* root, ZNode* nodeDelete) {
    if (!root) {
        return nullptr;
    }

    ZNode* rootData = container_of(root, ZNode, tree);

    // Compare nodes based on score, len, and name
    if (nodeDelete->score < rootData->score ||
        (nodeDelete->score == rootData->score && nodeDelete->len < rootData->len) ||
        (nodeDelete->score == rootData->score && nodeDelete->len == rootData->len &&
         strcmp(nodeDelete->name, rootData->name) < 0)) {
        root->left = avl_delete(root->left, nodeDelete);
    } else if (nodeDelete->score > rootData->score ||
               (nodeDelete->score == rootData->score && nodeDelete->len > rootData->len) ||
               (nodeDelete->score == rootData->score && nodeDelete->len == rootData->len &&
                strcmp(nodeDelete->name, rootData->name) > 0)) {
        root->right = avl_delete(root->right, nodeDelete);
    } else {
        // Node to be deleted found
        if (!root->left || !root->right) {
            AVLNode* temp = root->left ? root->left : root->right;

            if (!temp) {
                // No children
                temp = root;
                root = nullptr;
            } else {
                // One child
                *root = *temp;
            }

            //free(container_of(temp, ZNode, tree));
        } else {
            // Two children: Get the inorder successor
            AVLNode* temp = findMin(root->right);
            ZNode* tempData = container_of(temp, ZNode, tree);

            // Copy successor's data to current node
            rootData->score = tempData->score;
            rootData->len = tempData->len;
            memcpy(rootData->name, tempData->name, tempData->len + 1);

            // Delete the inorder successor
            root->right = avl_delete(root->right, tempData);
        }
    }

    if (!root) {
        return root;
    }

    // Update height and count
    updateNode(root);

    // Rebalance the tree
    int balance = getBalanceFactor(root);

    if (balance > 1) {
        if (getBalanceFactor(root->left) >= 0) {
            return rotateRight(root);
        } else {
            root->left = rotateLeft(root->left);
            return rotateRight(root);
        }
    }

    if (balance < -1) {
        if (getBalanceFactor(root->right) <= 0) {
            return rotateLeft(root);
        } else {
            root->right = rotateRight(root->right);
            return rotateLeft(root);
        }
    }

    return root;
}

AVLNode *avl_offset(AVLNode *node, int64_t offset) {
    int64_t pos = 0;    // relative to the starting node
    while (offset != pos) {
        if (pos < offset && pos + avl_getcount(node->right) >= offset) {
            // the target is inside the right subtree
            node = node->right;
            pos += avl_getcount(node->left) + 1;
        } else if (pos > offset && pos - avl_getcount(node->left) <= offset) {
            // the target is inside the left subtree
            node = node->left;
            pos -= avl_getcount(node->right) + 1;
        } else {
            // go to the parent
            AVLNode *parent = node->parent;
            if (!parent) {
                return NULL;
            }
            if (parent->right == node) {
                pos -= avl_getcount(node->left) + 1;
            } else {
                pos += avl_getcount(node->right) + 1;
            }
            node = parent;
        }
    }
    return node;
}

void inorderTraversal(AVLNode* root) {
    if (!root) {
        return;
    }
    inorderTraversal(root->left);
    ZNode* currentNode = container_of(root, ZNode, tree); 
    std::cout << currentNode->name << " " << currentNode->score << " ";
    inorderTraversal(root->right);
}








