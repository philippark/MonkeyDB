#include <stdlib.h>
#include <assert.h>
#include "hash_map.h"

// maximum load factor for a hashmap
const size_t K_MAX_LOAD_FACTOR = 8;

// number of keys to migrate during rehashing
const size_t K_REHASHING_WORK = 128;

// initialize hash table
void hash_init(HashTable *hash_table, size_t n) {
    assert(n > 0 && ((n - 1) & n) == 0); // check that n is a power of 2
    hash_table->table = (HashNode **)calloc(n, sizeof(HashNode *));
    hash_table->mask = n - 1;
    hash_table->size = n; 
}

// insert hash node into hash table
void hash_insert(HashTable *hash_table, HashNode *hash_node) {
    size_t pos = hash_node->hash_code & hash_table->mask;
    HashNode *next = hash_table->table[pos];
    hash_node->next = next;
    hash_table->table[pos] = hash_node;
    hash_table->size++; 
}

// finds and returns the address of the node if key exists in the hash table
static HashNode **hash_lookup(HashTable *hash_table, HashNode *key, bool (*eq)(HashNode*, HashNode*)) {
    if (!hash_table->table) {
        return nullptr;
    }

    size_t pos = key->hash_code & hash_table->mask;

    HashNode** curr_addr = &hash_table->table[pos];
    HashNode* curr_node = *curr_addr;

    while (curr_node != nullptr) {
        if (curr_node->hash_code == key->hash_code && eq(curr_node, key)) {
            // return the actual address for deletion instances
            return curr_addr; 
        }

        curr_addr = &curr_node->next;
        curr_node = *curr_addr;
    }

    return nullptr;
}

static HashNode* hash_detach(HashTable *table, HashNode **node_addr) {
    HashNode *node = *node_addr;
    *node_addr = node->next;
    table->size--;
    return node;
}

// handles rehashing the hashmap when it's overloaded
static void hash_map_rehash(HashMap *hash_map) {
    hash_map->older = hash_map->newer;
    hash_init(&hash_map->newer, (hash_map->newer.mask + 1) * 2);
    hash_map->migrate_pos = 0;
}

// migrates a constant number of items from older to newer table
void hash_map_migrate(HashMap *hash_map) {
    size_t migrated { 0 };

    while (migrated < K_REHASHING_WORK && hash_map->older.size > 0) {
        HashNode **node_addr { &hash_map->older.table[hash_map->migrate_pos] };

        // if empty node, skip it
        if (!*node_addr) {
            hash_map->migrate_pos++;
            continue;
        }

        // remove from older, and insert into newer
        HashNode* node { hash_detach(&hash_map->older, node_addr) };
        hash_insert(&hash_map->newer, node);
        migrated++;
    }

    // free older table if no more keys in it
    if (hash_map->older.size == 0 && hash_map->older.table) {
        free(hash_map->older.table);
        hash_map->older = HashTable{};
    }
}


// handles lookups for a key in a hashmap
// returns node in hashmap if key is found, else null
HashNode *hash_map_lookup(HashMap *hash_map, HashNode *key, bool (*eq)(HashNode *, HashNode *)) {
    // progressive migration
    hash_map_migrate(hash_map);
    
    HashNode **node_addr = hash_lookup(&hash_map->newer, key, eq);

    // if not in newer, check if in older
    if (!node_addr) {
        node_addr = hash_lookup(&hash_map->older, key, eq);
    }

    return node_addr ? *node_addr : nullptr;
}

// handles deleting a key from a hashmap
// returns deleted node from hashmap if found, returns null if node not found
HashNode *hash_map_delete(HashMap *hash_map, HashNode *key, bool (*eq)(HashNode *, HashNode *)) {
    // progressive migrating
    hash_map_migrate(hash_map);

    // check newer
    if (HashNode **node_addr = hash_lookup(&hash_map->newer, key, eq)) {
        return hash_detach(&hash_map->newer, node_addr);
    }
    
    // check older
    if (HashNode **node_addr = hash_lookup(&hash_map->older, key, eq)) {
        return hash_detach(&hash_map->older, node_addr);
    }

    return nullptr;
}

// inserts node into the hashmap
void hash_map_insert(HashMap *hash_map, HashNode *node) {
    // initialize if no newer table
    if (!hash_map->newer.table) {
        hash_init(&hash_map->newer, 4);
    }

    hash_insert(&hash_map->newer, node);

    // rehash if too much keys in newer table
    if (!hash_map->older.table) {
        size_t threshold { (hash_map->newer.mask + 1) * K_MAX_LOAD_FACTOR };
        if (hash_map->newer.size >= threshold) {
            hash_map_rehash(hash_map);
        }
    }

    // progressive migration
    hash_map_migrate(hash_map);
}