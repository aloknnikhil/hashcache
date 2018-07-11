#include <iostream>
#include <vector>
#include <thread>
#include <future>
#include <mutex>
#include <memory>
#include <functional>
#include <csignal>

using namespace std;
using namespace std::chrono;

/**
 * Base node for defining the structure of a Binary Search Tree
 * Supports templated Key Value Pairs
 *
 */
template <typename K, typename V>
class HashTree {
public:
  K m_key;
  V m_val;
  HashTree* m_left;
  HashTree* m_right;

  HashTree(K key, V val) : m_key(key), m_val(val), m_left(nullptr), m_right(nullptr) {}

  /**
   * BST insertion - O(log n)
   *
   */
  static HashTree* insertNode(HashTree* root, const K& key, const V& val) {
    if (root == nullptr) {
      root = new HashTree(key, val);
    } else if (root->m_key >= key) {
      root->m_left = insertNode(root->m_left, key, val);
    } else {
      root->m_right = insertNode(root->m_right, key, val);
    }

    return root;
  }

  /**
   * BST lookup - O(log n)
   *
   */
  static bool getVal(HashTree* root, const K& key, V& val) {
    if (root == nullptr) {
      return false;
    }

    if (root->m_key == key) {
      val = root->m_val;
      return true;
    }

    if (root->m_key > key) {
      return getVal(root->m_left, key, val);
    } else {
      return getVal(root->m_right, key, val);
    }
  }

  /**
   * Get smallest node - O(n)
   *
   */
  static HashTree* getSmallestNode(HashTree* root) {
    HashTree* current = root;
    while (current && current->m_left) {
      current = current->m_left;
    }

    return current;
  }

  /**
   * BST removal - O(log n)
   *
   */
  static HashTree* remove(HashTree* root, const K& key) {
    if (root == nullptr) {
      return nullptr;
    }

    if (root->m_key > key) {
      root->m_left = remove(root->m_left, key);
    } else if (root->m_key < key) {
      root->m_right = remove(root->m_right, key);
    } else {
      if (root->m_left == nullptr) {
        HashTree* temp = root->m_right;
        delete root;
        return temp;
      } else if (root->m_right == nullptr) {
        HashTree* temp = root->m_left;
        delete root;
        return temp;
      }

      // Find largest node in the left tree
      HashTree* inOrderSuccessor = getSmallestNode(root->m_right);
      root->m_key = inOrderSuccessor->m_key;
      root->m_val = inOrderSuccessor->m_val;
      root->m_right = remove(root->m_right, inOrderSuccessor->m_key);
    }

    return root;
  }

  /**
   * BST comparator - O(n)
   * Input: Function to compare elements with
   *
   */
  static HashTree* seekWithComparator(HashTree* root, function<HashTree*(HashTree*, HashTree*)>& comparator) {
    if (root == nullptr) {
      return nullptr;
    }

    // Search on the left Tree
    HashTree* left = seekWithComparator(root->m_left, comparator);
    HashTree* right = seekWithComparator(root->m_right, comparator);

    return comparator(comparator(left, right), root);
  }
};
/**
 * Cache implementation
 * Designed to support O(1) insertion, O(1) lookup, O(1) update, O(n) deletion
 *
 */
template <typename K, typename V>
class Cache {
protected:
  /**
   * Partitions in the cache - Not necesarrily same as number of elements
   * Increase to have a better average case insertion/lookup performance
   *
   */
  static const long NUM_BUCKETS = 1024;

  /**
   * Maximum number of elements supported by the cache
   *
   */
  static const long CACHE_SIZE = 1024;

  /**
   * Cache partitions - Same as number of slots in the cache if NUM_BUCKETS = CACHE_SIZE
   *
   */
  HashTree<K, pair<V, long>>* buckets[NUM_BUCKETS];

  /**
   * Locks to protect access to a partition
   *
   */
  mutex bucketLocks[NUM_BUCKETS];

  /**
   * Actual number of elements in the cache
   *
   */
  atomic<long> cacheSize;

  int hashFunc(const K& key) {
    hash<K> hashVal;
    return (hashVal(key) % NUM_BUCKETS);
  }

public:
  Cache() {
    for (int i = 0; i < NUM_BUCKETS; i++) {
      buckets[i] = nullptr;
    }
    cacheSize = 0;
  }

  bool get(const K& key, V& val) {
    int hashVal = hashFunc(key);
    // Acquire bucket lock
    scoped_lock<mutex> lock(bucketLocks[hashVal]);
    HashTree<K, pair<V, long>>* bucket = buckets[hashVal];

    pair<V, long> valWrapper;

    bool ret = HashTree<K, pair<V, long>>::getVal(bucket, key, valWrapper);
    val = valWrapper.first;

    return ret;
  }

  bool put(const K& key, const V& val) {

    if (cacheSize.fetch_add(1) >= CACHE_SIZE) {
      removeLRU();
    }

    int hashVal = hashFunc(key);
    // Acquire bucket lock
    scoped_lock<mutex> lock(bucketLocks[hashVal]);
    HashTree<K, pair<V, long>>* bucket = buckets[hashVal];

    long currentTimeMillis = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    if (cacheSize.load() == 1) {
      cout << "First element timestamp " << currentTimeMillis << endl;
    }

    buckets[hashVal] = HashTree<K, pair<V, long>>::insertNode(bucket, key, pair<V, long>(val, currentTimeMillis));

    return true;
  }

  bool remove(const K& key) {
    int hashVal = hashFunc(key);
    // Acquire bucket lock
    scoped_lock<mutex> lock(bucketLocks[hashVal]);
    HashTree<K, pair<V, long>>* bucket = buckets[hashVal];

    buckets[hashVal] = HashTree<K, pair<V, long>>::remove(bucket, key);

    cacheSize.fetch_sub(1);

    return true;
  }

  bool removeLRU() {
    // Search each bucket for oldest -> O(n)
    // Get key for oldest -> O(1)
    // Remove key -> O(log n)
    function<HashTree<K, pair<V, long>>* (HashTree<K, pair<V, long>>*, HashTree<K, pair<V, long>>*)> func =
    [](HashTree<K, pair<V, long>>* left, HashTree<K, pair<V, long>>* right) {
      if (left == nullptr) {
        return right; // Could also be null
      }

      if (right == nullptr) {
        return left; // Could also be null
      }
      // If both are null, we would return null anyway
      // Else return older of the two
      return (left->m_val.second < right->m_val.second ? left : right);
    };

    HashTree<K, pair<V, long>>* oldest = nullptr;

    for (int i = 0; i < NUM_BUCKETS; i++) {
      HashTree<K, pair<V, long>>* currentOldest;
      scoped_lock<mutex> lock(bucketLocks[i]);
      HashTree<K, pair<V, long>>* bucket = buckets[i];
      currentOldest = HashTree<K, pair<V, long>>::seekWithComparator(bucket, func);

      if (!currentOldest) {
        continue;
      }

      if (!oldest) {
        oldest = currentOldest;
        continue;
      }

      if (currentOldest->m_val.second < oldest->m_val.second) {
        oldest = currentOldest;
      }
    }

    cout << "Removing oldest element " << oldest->m_key << " w/ timestamp " << oldest->m_val.second << endl;
    remove(oldest->m_key);
  }
};

// TEST PROGRAM

#define MAX_ELEMENTS        1024
#define MAX_THREADS         4
#define ELEMENTS_PER_THREAD MAX_ELEMENTS/MAX_THREADS

struct Element {
  int val1;
  char val2;
  int val3;

  Element () : val1(0), val2('\0'), val3(0) {}
  Element (int in_val1, char in_val2, int in_val3) : val1(in_val1), val2(in_val2), val3(in_val3) {}
};

int main (int argc, char** argv) {
  Cache<long, shared_ptr<Element>>* cache = new Cache<long, shared_ptr<Element>>();
  vector<int> keys(MAX_ELEMENTS);

  /**
   * Populates cache with MAX_ELEMENTS
   *
   */
  auto insertFunc = [&](int tid) {
    for (int i = (tid * ELEMENTS_PER_THREAD); i < ((tid * ELEMENTS_PER_THREAD) + ELEMENTS_PER_THREAD); i++) {
      if (i >= MAX_ELEMENTS) {
        break;
      }
      int randKey = rand();
      keys[i] = randKey;
      if (!cache->put(randKey, make_shared<Element>(i, '\0', randKey))) {
        cout << "Cache full! ERROR!" << endl;
        break;
      }
    }
  };

  /**
   * Multi-threaded insert into the cache
   *
   */
  vector<future<void>> futures;

  for (int i = 0; i < MAX_THREADS; i++) {
    futures.push_back(async(insertFunc, i));
  }

  for (auto& future : futures) {
    future.get();
  }

  futures.clear();

  /**
   * Retrieves from cache and updates element field
   *
   */
  auto updateFunc = [&](int tid) {
    for (const auto& key : keys) {
      shared_ptr<Element> element;
      if (cache->get(key, element)) {
        element->val3 = rand();
      } else {
        cout << "[UPDATE] ERROR! Element " << key << " not found in cache. Possibly evicted?" << endl;
      }
    }
  };

  for (int i = 0; i < MAX_THREADS; i++) {
    futures.push_back(async(updateFunc, i));
  }

  for (auto& future : futures) {
    future.get();
  }

  futures.clear();

  /**
   * Removes element from cache
   *
   */
  auto deleteFunc = [&](int tid) {
    for (const auto& key : keys) {
      if (!cache->remove(key)) {
        cout << "[DELETE] ERROR! Element " << key << " not found in cache. Possibly evicted?" << endl;
      }
    }
  };

  // UNCOMMENT BELOW TO TEST SIMULTANEOUS UPDATES & DELETES  
  /*for (int i = 0; i < MAX_THREADS; i++) {
    futures.push_back(async(deleteFunc, i));
  }

  for (int i = 0; i < MAX_THREADS; i++) {
    futures.push_back(async(updateFunc, i));
  }

  for (auto& future : futures) {
    future.get();
  }

  futures.clear();*/

  return 0;
}
