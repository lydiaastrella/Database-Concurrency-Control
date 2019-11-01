#include <bits/stdc++.h>
// #include <unordered_map>
// #include <deque>
using std::unordered_map;
using std::deque;
// using namespace std;

typedef uint64_t Key;
typedef uint64_t Value;

struct Version {
	Value value_;      // The value of this version
	int max_read_id_;  // Largest timestamp of a transaction that read the version
	int version_id_;   // Timestamp of the transaction that created(wrote) the version
};

unordered_map<int, deque<Version*>*> mvcc_data_;

// Debug procedure
void print_deque(deque<Version*>* deque_) {
	for (deque<Version*>::iterator it = deque_->begin(); it != deque_->end(); ++it) {
		std::cout << "Value: " <<  (*it)->value_ << " max_read_id_: " << (*it)->max_read_id_ << " version_id_: " << (*it)->version_id_ << std::endl;
	}
}

bool Read(Key key, Value *result, int txn_unique_id) {
	// Implement this method!
}

// Check whether apply or abort the write
bool CheckWrite(Key key, int txn_unique_id) {
  // Implement this method!
}

void Write(Key key, Value value, int txn_unique_id) {
    // Implement this method!
}

int main ()
{
	deque<Version*> mydeque;
	deque<Version*> mydeque2;
	deque<Version*> mydeque3;

	// Version v1;
	// v1.value_ = 1;
	// v1.max_read_id_ = 2;
	// v1.version_id_ = 3;

	// Version v2;
	// v2.value_ = 2;
	// v2.max_read_id_ = 4;
	// v2.version_id_ = 5;

	// Version v3;
	// v3.value_ = 3;
	// v3.max_read_id_ = 6;
	// v3.version_id_ = 7;

	// Version v4;
	// v4.value_ = 4;
	// v4.max_read_id_ = 8;
	// v4.version_id_ = 1000000;

	// mydeque.push_back(&v1);
	// mydeque.push_back(&v2);
	// mydeque.push_back(&v3);

	// mydeque2.push_back(&v4);

	mvcc_data_[1] = &mydeque;
	mvcc_data_[2] = &mydeque2;
	mvcc_data_[3] = &mydeque3;

	for (int i = 1; i <= 3; i++) {
		Write(i, 0, 0);
		print_deque(mvcc_data_[i]);
	}

	for (int i = 0; i < 100000; i++) {
		Key key = 0;
		Value value = 0;
		int txn_unique_id = 0;
		std::cin >> key;
		std::cin >> value;
		std::cin >> txn_unique_id;
		
		Write(key, value, txn_unique_id);
		print_deque(mvcc_data_[key]);
	}
}