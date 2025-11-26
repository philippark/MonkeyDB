#pragma once

#include <stddef.h>
#include <stdint.h>

// hashtable node, must be embedded into the payload
struct HashNode {
    HashNode *next;
    uint64_t hash_code { 0 }; // hash value
};

// fix-sized hashtable
struct HashTable {
    HashNode **table { nullptr };
    size_t mask { 0 }; // power of 2 of the array size
    size_t size = { 0 }; // number of keys in the table
};

// Re-sizable hashmap
struct HashMap {
    HashTable newer;
    HashTable older;
    size_t migrate_pos { 0 };
};

HashNode *hash_map_lookup(HashMap *hash_map, HashNode *key, bool (*eq)(HashNode *, HashNode *));
HashNode *hash_map_delete(HashMap *hash_map, HashNode *key, bool (*eq)(HashNode *, HashNode *));
void hash_map_insert(HashMap *hash_map, HashNode *node);