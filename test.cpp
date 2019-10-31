#include <bits/stdc++.h>
// #include <unordered_map>
// #include <deque>
using std::unordered_map;
using std::deque;
// using namespace std;
struct Version {
	int value_;      // The value of this version
	int max_read_id_;  // Largest timestamp of a transaction that read the version
	int version_id_;   // Timestamp of the transaction that created(wrote) the version
};

typedef uint64_t Key;
typedef uint64_t Value;

unordered_map<int, deque<Version*>*> mvcc_data_;

int FetchLargestVersionID(Key key, int txn_unique_id) {
  int max_version_id_= -99999;
  for (deque<Version*>::iterator it = mvcc_data_[key]->begin(); it != mvcc_data_[key]->end(); ++it) {
    if ((*it)->version_id_ <= txn_unique_id && (*it)->version_id_ > max_version_id_) {
      max_version_id_ = (*it)->version_id_;
    }
  }
  return max_version_id_;
}

void print_deque(deque<Version*>* deque_) {
	// for (int i = 0; i < deque_->size() ; i++) {
	// 	// std::cout << deque_->size() << std::endl;
	// 	std::cout << deque_[i]->version_id_ << std::endl;
	// }
	for (deque<Version*>::iterator it = deque_->begin(); it != deque_->end(); ++it) {
		std::cout << "Value: " <<  (*it)->value_ << " max_read_id_: " << (*it)->max_read_id_ << " version_id_: " << (*it)->version_id_ << std::endl;
	}
 	//
}

bool Read(Key key, Value *result, int txn_unique_id) {
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
	        (*it)->max_read_id_ = txn_unique_id;
	        break;
	    }
	  }
	  return true;
}

// Check whether apply or abort the write
bool CheckWrite(Key key, int txn_unique_id) {
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
	// std::cout << mvcc_data_[key]->empty() << std::endl;
	if (mvcc_data_[key]->empty()) {
		std::cout <<"hheee" << std::endl;
    return true;
  }
  int max_version_id_ = FetchLargestVersionID(key, txn_unique_id);
  // If no write timestamp that is less than or equal to txn_unique_id
  if (max_version_id_ == -99999) {
    return false;
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

		// Value b = 0;
		// int c = 0;
		// std::cin >> a;
		// std::cin >> b;
		// std::cin >> c;
		// // Read(a, &b, c);
		// Write(a, b, c);
void Write(Key key, Value value, int txn_unique_id) {
    if (CheckWrite(key, txn_unique_id)) {
    // Fetch largest version id.
    // std::cout << mvcc_data_[key]->empty() << std::endl;
   if (mvcc_data_[key]->empty()) {
      Version* new_version = new Version;
      new_version->max_read_id_ = 0;
      new_version->value_ = value;
      new_version->version_id_ = txn_unique_id;
      mvcc_data_[key]->push_back(new_version);
      return;
    }
    int max_version_id_ = FetchLargestVersionID(key, txn_unique_id);
    Version* new_version = new Version;
    for (deque<Version*>::iterator it = mvcc_data_[key]->begin(); it != mvcc_data_[key]->end(); ++it) {
        if (max_version_id_ == (*it)->version_id_) {
            // Allocation of new version.
            new_version->max_read_id_ = (*it)->max_read_id_;
            new_version->value_ = value;
            new_version->version_id_ = txn_unique_id;
            break;
        }
    }
    // Then insert to deque or overwrite existing value
    bool exist = false;
    for (deque<Version*>::iterator it = mvcc_data_[key]->begin(); it != mvcc_data_[key]->end(); ++it) {
        if ((*it)->version_id_ == txn_unique_id) {
            // Overwrite
            (*it)->value_ = value;
            exist = true;
            break;
        }
    }
    if (!exist) {
        mvcc_data_[key]->push_back(new_version);
    }
  }
}

int main ()
{
	deque<Version*> mydeque;
	deque<Version*> mydeque2;
	deque<Version*> mydeque3;

	Version v1;
	v1.value_ = 1;
	v1.max_read_id_ = 2;
	v1.version_id_ = 3;

	Version v2;
	v2.value_ = 2;
	v2.max_read_id_ = 4;
	v2.version_id_ = 5;

	Version v3;
	v3.value_ = 3;
	v3.max_read_id_ = 6;
	v3.version_id_ = 7;

	Version v4;
	v4.value_ = 4;
	v4.max_read_id_ = 8;
	v4.version_id_ = 9;

	// mydeque.push_back(&v1);
	// mydeque.push_back(&v2);
	// mydeque.push_back(&v3);

	// mydeque2.push_back(&v4);

	mvcc_data_[1] = &mydeque;
	mvcc_data_[2] = &mydeque2;
	mvcc_data_[3] = &mydeque3;
	// print_deque(m[1]);
	int result = -1;
	// bool check = Read(2, &result, 4);
	// std::cout << check << std::endl;
	// std::cout << "testing " << result << std::endl;
	for (int i = 1; i <= 3; i++) {
		// Key a = 0;
		// Value b = 0;
		// int c = 0;
		// std::cin >> a;
		// std::cin >> b;
		// std::cin >> c;
		// // Read(a, &b, c);
		// Write(a, b, c);
		// std::cout << "i : " << i << std::endl;
		Write(i, 0, 0);
		// std::cout << "Read: " << b << std::endl;
		print_deque(mvcc_data_[i]);
		// Write(a, b, c);
	}

	for (int i = 0; i < 100000; i++) {
		Key a = 0;
		Value b = 0;
		int c = 0;
		std::cin >> a;
		std::cin >> b;
		std::cin >> c;
		// Read(a, &b, c);
		Write(a, b, c);
		// std::cout << "i : " << i << std::endl;
		// std::cout << "Read: " << b << std::endl;
		// Write(a, b, c);
		print_deque(mvcc_data_[a]);
	}
	// for (deque<Version>::iterator it = mydeque.begin(); it != mydeque.end(); ++it)
 //    	std::cout << ' ' << *it;
 //  	std::cout << '\n';
	// int a = 2;
	// int b = 3;
	// int c = 4;
	// m[1] = a;
	// m[2] = b;
	// m[3] = c;
	// if (m.find(5) == m.end())	{
	// 	std::cout << "Ah sed" << std::endl;
	// }
	// std::cout << m[1] << std::endl;
	// cout << "Ga ketemu" << endl;
}