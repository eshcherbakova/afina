#include "SimpleLRU.h"

#include <iostream>
#include <utility>

namespace Afina {
namespace Backend {

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Put(const std::string &key, const std::string &value) {

    if (key.size() + value.size() > _max_size) {
        return false;
    }
    
    auto iter = _lru_index.find(key);

    if (iter == _lru_index.end()) {
        return PutIfAbsent(key, value);
    }

    move_to_tail(iter);

    while (_cur_size + value.size() - iter->second.get().value.size() > _max_size) {
        delete_old_node();
    }

    _cur_size = _cur_size + value.size() - iter->second.get().value.size();
    iter->second.get().value = value;
    iter->second.get().key = key;
    return true;
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::PutIfAbsent(const std::string &key, const std::string &value) {
    
    
    if (key.size() + value.size() > _max_size) {
        return false;
    }
    
    auto iter = _lru_index.find(key);

    if (iter != _lru_index.end()) {
        return false;
    }

    while (key.size() + value.size() + _cur_size > _max_size) {
        delete_old_node();
    }

    _cur_size += key.size() + value.size();
    
    auto new_node = new lru_node{key, value, nullptr, nullptr}; 
    auto ptr = std::unique_ptr<lru_node>(new_node);
    
    if (_lru_tail == nullptr) {
       _lru_head = std::move(ptr);
       _lru_tail = _lru_head.get();
    } else {
        ptr->prev = _lru_tail;
        _lru_tail->next = std::move(ptr);
        _lru_tail = _lru_tail->next.get();
    }
    _lru_index.emplace(new_node->key,
                       *new_node);
    return true;
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Set(const std::string &key, const std::string &value) {
    auto iter = _lru_index.find(key);
    if (iter == _lru_index.end()) {
        return false;
    }

    if (key.size() + value.size() > _max_size) {
        return false;
    }

    move_to_tail(iter);

    while (value.size() - iter->second.get().value.size() + _cur_size > _max_size) {
        delete_old_node();
    }
    _cur_size = _cur_size + value.size() - iter->second.get().value.size();
    iter->second.get().value = value;
    iter->second.get().key = key;
    return true;
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Delete(const std::string &key) {
    auto iter = _lru_index.find(key);
    if (iter == _lru_index.end()) {
        return false;
    }

    lru_node &del_node = iter->second.get();
    _lru_index.erase(key);
    
    if (del_node.prev == nullptr) { //deleting head
        _lru_head = std::move(_lru_head->next);
        _lru_head->prev = nullptr;
    } else if (del_node.next == nullptr) { //deleting tail
        lru_node* new_tail = _lru_tail->prev;
        new_tail->next = nullptr;
        _lru_tail = new_tail;
    } else {
        lru_node* prev = del_node.prev;
        prev->next = std::move(del_node.next);
        prev->next->prev = prev;
    }
    return true;
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Get(const std::string &key, std::string &value) {
    auto iter = _lru_index.find(std::reference_wrapper<const std::string>(key));
    if (iter == _lru_index.end()) {
        return false;
    }
    value = iter->second.get().value;
    return true;
}

void SimpleLRU::delete_old_node() {

    if (_lru_tail == nullptr) {
        return;
    }
    _cur_size -= _lru_head->key.size() + _lru_head->value.size();
    _lru_index.erase(_lru_head->key);
    
    if (_lru_head->next != nullptr) {
        _lru_head = std::move(_lru_head->next);
        _lru_head->prev = nullptr;
    } else {
        _lru_head = nullptr;
        _lru_tail = nullptr;
    }     
}

void SimpleLRU::move_to_tail(std::map<std::reference_wrapper<const std::string>, std::reference_wrapper<lru_node>,
                                      std::less<std::string>>::iterator &iter) {

   lru_node& node_move = iter->second.get();
    
   if (node_move.next != nullptr) { //isn't last
        auto ptr = node_move.next->prev;
        ptr->next->prev = ptr->prev;
        if (ptr->prev != nullptr) {//in the middle
            _lru_tail->next = std::move(ptr->prev->next);
            ptr->prev->next = std::move(ptr->next);
        } else {//head case
            _lru_tail->next = std::move(_lru_head);
            _lru_head = std::move(ptr->next);
        }
        ptr->next = nullptr;
        ptr->prev = _lru_tail;
        _lru_tail = ptr;
    }
    
    
    
    
}


} // namespace Backend
} // namespace Afina
