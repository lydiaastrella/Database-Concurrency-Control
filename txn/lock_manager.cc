// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManager::~LockManager() {
  // Cleanup lock_table_
  for (auto it = lock_table_.begin(); it != lock_table_.end(); it++) {
    delete it->second;
  }
}

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn *txn, const Key &key)
{
  //
  // Implement this method!
  LockRequest lr(EXCLUSIVE, txn);
  if (lock_table_.find(key) == lock_table_.end())
  {
    deque<LockRequest> *d = new deque<LockRequest>();
    d->push_back(lr);
    lock_table_[key] = d;
    return true;
  }
  else
  {
    if(lock_table_[key]->empty())
    {
      lock_table_[key]->push_back(lr);
      return true;
    }else{
      lock_table_[key]->push_back(lr);
      txn_waits_[txn] += 1;
      return false;
    }
  }
}

bool LockManagerA::ReadLock(Txn *txn, const Key &key)
{
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn *txn, const Key &key)
{
  //
  // Implement this method!
  uint p = -1;
  for (uint i = 0; i < lock_table_[key]->size(); i++)
  {
    if (lock_table_[key]->operator[](i).txn_ == txn)
    {
		  p = i;
      break;
    }
  }
  if(p != lock_table_[key]->size()){
    lock_table_[key]->erase(lock_table_[key]->begin()+p);
  }
  if(p == 0 && !lock_table_[key]->empty()){
    //if (txn_waits_[lock_table_[key]->front().txn_] > 0){
    txn_waits_[lock_table_[key]->front().txn_]--;
    //}

    if (txn_waits_[lock_table_[key]->front().txn_] == 0)
    {
      ready_txns_->push_back(lock_table_[key]->front().txn_);
      txn_waits_.erase(lock_table_[key]->front().txn_);
    }
  }
}

LockMode LockManagerA::Status(const Key &key, vector<Txn *> *owners)
{
  //
  // Implement this method!
  owners->clear();
  if (lock_table_.find(key) == lock_table_.end()){
    vector<Txn *> *t = new vector<Txn *>();
    owners = t;
    return UNLOCKED;
  }else{
    //for (uint i = 0; i < lock_table_[key]->size(); i++){
	
      owners->push_back(lock_table_[key]->operator[](0).txn_);
    //}
    return EXCLUSIVE;
  }
    return UNLOCKED;
}

/*
LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::_addLock(LockMode mode, Txn* txn, const Key& key) {
  LockRequest rq(mode, txn);
  LockMode status = Status(key, nullptr);

  deque<LockRequest> *dq = _getLockQueue(key);
  dq->push_back(rq);

  bool granted = status == UNLOCKED;
  if (mode == SHARED) {
    granted |= _noExclusiveWaiting(key);
  } else {
    _numExclusiveWaiting[key]++;
  }

  if (!granted)
    txn_waits_[txn]++;

  return granted;
}


bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  return _addLock(EXCLUSIVE, txn, key);
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  return _addLock(SHARED, txn, key);
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  deque<LockRequest> *queue = _getLockQueue(key);

  for (auto it = queue->begin(); it < queue->end(); it++) {
    if (it->txn_ == txn) {
      queue->erase(it);
      if (it->mode_ == EXCLUSIVE) {
        _numExclusiveWaiting[key]--;
      }

      break;
    }
  }

  // Advance the lock, by making new owners ready.
  // Some in newOwners already own the lock.  These are not in
  // txn_waits_.
  vector<Txn*> newOwners;
  Status(key, &newOwners);

  for (auto&& owner : newOwners) {
    auto waitCount = txn_waits_.find(owner);
    if (waitCount != txn_waits_.end() && --(waitCount->second) == 0) {
      ready_txns_->push_back(owner);
      txn_waits_.erase(waitCount);
    }
  }
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest> *dq = _getLockQueue(key);
  if (dq->empty()) {
    return UNLOCKED;
  }

  LockMode mode = EXCLUSIVE;
  vector<Txn*> txn_owners;
  for (auto&& lockRequest : *dq) {
    if (lockRequest.mode_ == EXCLUSIVE && mode == SHARED)
        break;

    txn_owners.push_back(lockRequest.txn_);
    mode = lockRequest.mode_;

    if (mode == EXCLUSIVE)
      break;
  }

  if (owners)
    *owners = txn_owners;

  return mode;
}

inline bool LockManagerB::_noExclusiveWaiting(const Key& key) {
  return _numExclusiveWaiting[key] == 0;
}
*/