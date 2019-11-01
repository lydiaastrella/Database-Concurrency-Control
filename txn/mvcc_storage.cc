// Author: Kun Ren (kun.ren@yale.edu)
// Modified by Daniel Abadi

#include "txn/mvcc_storage.h"

// Fetch largest write timestamp (version_id_) and return it.
// Precondition: Key already exists in a map.
int MVCCStorage::FetchLargestVersionID(Key key, int txn_unique_id) {
    int max_version_id_= -99999;
    if (mvcc_data_.find(key) == mvcc_data_.end()) {
        return -99999;
    }
    for (deque<Version*>::iterator it = mvcc_data_[key]->begin(); it != mvcc_data_[key]->end(); ++it) {
        if ((*it)->version_id_ <= txn_unique_id && (*it)->version_id_ > max_version_id_) {
            max_version_id_ = (*it)->version_id_;
        }
    }
    return max_version_id_;
}

// Init the storage
void MVCCStorage::InitStorage() {
	for (int i = 0; i < 1000000;i++) {
    	Write(i, 0, 0);
    	Mutex* key_mutex = new Mutex();
    	mutexs_[i] = key_mutex;
  	}
}

// Free memory.
MVCCStorage::~MVCCStorage() {
	for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin(); it != mvcc_data_.end(); ++it) {
    	// std::cout << "key: " << it->first << std::endl;
		// for (deque<Version*>::iterator itr = mvcc_data_[it->first]->begin(); itr != mvcc_data_[it->first]->end(); ++it) {
			// std::cout << "itr value: " << (*itr)->version_id_ << std::endl;
			// delete (*itr);
		// }
		// std::cout << "attempt to delete: " << std::endl;
		delete it->second;
  }
	mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  // Hint: Iterate the version_lists and return the verion whose write timestamp
  // (version_id) is the largest write timestamp less than or equal to txn_unique_id.

  // Check if key exists
  if (mvcc_data_.find(key) == mvcc_data_.end()) {
    return false;
  }

  // Fetch largest version id
  int max_version_id_ = FetchLargestVersionID(key, txn_unique_id);
  
  // If no write timestamp that is less than or equal to txn_unique_id
  if (max_version_id_ == -99999) {
    return false;
  }

  // Fetch the value and update the max_read_id.
  for (deque<Version*>::iterator it = mvcc_data_[key]->begin(); it != mvcc_data_[key]->end(); ++it) {
    if (max_version_id_ == (*it)->version_id_) {
        *result = (*it)->value_;
        // Overwrite  largest max_read_id_
        if (txn_unique_id > (*it)->max_read_id_) {
        	(*it)->max_read_id_ = txn_unique_id;
        }
            break;
        }
  }
  return true;
}


// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Before all writes are applied, we need to make sure that each write
  // can be safely applied based on MVCC timestamp ordering protocol. This method
  // only checks one key, so you should call this method for each key in the
  // write_set. Return true if this key passes the check, return false if not. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  
  // Fetch largest version id
  int max_version_id_ = FetchLargestVersionID(key, txn_unique_id);
  // If no write timestamp that is less than or equal to txn_unique_id
  if (max_version_id_ == -99999) {
    return true;
  }
  for (deque<Version*>::iterator it = mvcc_data_[key]->begin(); it != mvcc_data_[key]->end(); ++it) {
    if (max_version_id_ == (*it)->version_id_) {
        // Check read timestamp
        if (txn_unique_id < (*it)->max_read_id_) {
          return false;
        }
        break;
    }
  }
  return true;
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
    // CPSC 438/538:
    //
    // Implement this method!
    // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
    // into the version_lists. Note that InitStorage() also calls this method to init storage. 
    // Note that you don't have to call Lock(key) in this method, just
    // call Lock(key) before you call this method and call Unlock(key) afterward.
    
    // Fetch largest version id.
    Version* new_version = new Version;
    int max_version_id_ = FetchLargestVersionID(key, txn_unique_id);
	// std::cout << "max_version_id: " << max_version_id_ << std::endl;
    // If is empty or not found, write
    if (max_version_id_ == -99999) {
        // If key does not exist then
        if (mvcc_data_.find(key) == mvcc_data_.end()) {
            // Allocate a new deque
            deque<Version*>* deque = new std::deque<Version*>;
            mvcc_data_[key] = deque;
        }
        new_version->max_read_id_ = txn_unique_id;
        new_version->value_ = value;
        new_version->version_id_ = txn_unique_id;
        mvcc_data_[key]->push_back(new_version);
        return;
      }
      for (deque<Version*>::iterator it = mvcc_data_[key]->begin(); it != mvcc_data_[key]->end(); ++it) {
          if (max_version_id_ == (*it)->version_id_) {
              if (max_version_id_ == txn_unique_id) {
                  // Overwrite
                  (*it)->value_ = value;
              } else {
                  // Allocation of new version.
                  new_version->max_read_id_ = txn_unique_id;
                  new_version->value_ = value;
                  new_version->version_id_ = txn_unique_id;
                  mvcc_data_[key]->push_back(new_version);
              }
            return;
          }
      }
}


