//
// fort: Constant-memory store for keys
//
// -----------------------------------------------------------------------------
//
// Copyright 2016 Ben Schofield
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "KeyStore.hpp"

#include <algorithm>
#include <cstring>

#include <iostream>

namespace Fort
{
  // ---- Constructors/destructors ----

  KeyStore::KeyStore(uint64_t size, const char* locale_name)
  {
    // Grab memory for the buffer
    buffer_size_ = size;
    buffer_base_ = new char[buffer_size_];

    // Buffer is initially empty
    lo_fill_ = 0;
    key_off_ = buffer_size_;

    // Count bits required to represent max possible offset
    off_bit_count_ = 0;

    while( size )
    {
      ++off_bit_count_;
      size >>= 1;
    } 

    // Compute offset and length masks
    off_mask_ = (1 << off_bit_count_) - 1;

    // Store max key length
    max_key_len_ = UINT64_C(0xffffffffffffffff) >> off_bit_count_;

    // If specified, set locale for sort
    if( locale_name )
    {
      loc_ = new std::locale(locale_name);
      coll_ = const_cast<std::collate<char>*>
                ( &std::use_facet< std::collate<char> >(*loc_) );
    }
    else
    {
      // Locale not to be used
      loc_ = nullptr;
      coll_ = nullptr;
    }
  }

  KeyStore::~KeyStore()
  {
    // Release buffer
    delete[] buffer_base_;

    // Drop locale
    if( loc_ )
    {
      delete loc_;
    }
  }

  KeyStore::KeyStore(KeyStore&& other)
    : buffer_base_(other.buffer_base_),
      buffer_size_(other.buffer_size_),
      lo_fill_(other.lo_fill_),
      key_off_(other.key_off_),
      off_bit_count_(other.off_bit_count_),
      off_mask_(other.off_mask_),
      max_key_len_(other.max_key_len_),
      loc_(other.loc_),
      coll_(other.coll_)
  {
    // Invalidate the source object
    other.buffer_base_ = nullptr;
    other.buffer_size_ = 0;
    other.loc_ = nullptr;
    other.coll_ = nullptr;
    other.clear();
  }

  // ---- Public member functions ----

  bool KeyStore::empty() const
  {
    return (lo_fill_ == 0) ? true : false;
  }

  uint64_t KeyStore::max_key_len() const
  {
    return max_key_len_;
  }

  uint64_t KeyStore::key_space() const
  {
    // Must allow space for an ol for the new key
    if( (key_off_ - lo_fill_) < sizeof(uint64_t) )
    {
      return 0;
    }

    return ( key_off_ - lo_fill_ - sizeof(uint64_t) );
  }

  const KeyStore::Iterator KeyStore::begin() const
  {
    return KeyStore::Iterator(*this, 0);
  }

  const KeyStore::Iterator KeyStore::end() const
  {
    return KeyStore::Iterator(*this, lo_fill_);
  }
  
  KeyStore::ReturnCode KeyStore::insert(const char* key, uint64_t key_len)
  {
    // Check we can store this
    if( key_len > max_key_len_ )
    {
      return KeyTooLong;
    }

    if( key_len > this->key_space() )
    {
      return NotEnoughSpace;
    }

    // Store this key
    key_off_ -= key_len;

    memcpy(buffer_base_ + key_off_, key, key_len);

    // Store offset-length
    *reinterpret_cast<uint64_t*>(buffer_base_ + lo_fill_) = 
      ( key_len << off_bit_count_) | key_off_;

    lo_fill_ += sizeof(uint64_t);

    return Inserted;
  }

  KeyStore::ReturnCode KeyStore::insert(std::string &key)
  {
    return this->insert(key.data(), key.size());
  }

  void KeyStore::sort()
  {
    std::sort(reinterpret_cast<uint64_t *>(buffer_base_),
              reinterpret_cast<uint64_t *>(buffer_base_ + lo_fill_),
              KeyStore::Sorter(*this));

    return;
  }

  void KeyStore::clear()
  {
    lo_fill_ = 0;
    key_off_ = buffer_size_;
  }

  // ---- Iterator ----

  // Constructor
  KeyStore::Iterator::Iterator(const KeyStore& keystore, uint64_t lo_itoff)
    : keystore_(keystore), lo_itoff_(lo_itoff)
  { }

  // Copy constructor
  KeyStore::Iterator::Iterator(const Iterator& it)
    : keystore_(it.keystore_), lo_itoff_(it.lo_itoff_)
  { }

  // Note Iterator not invalidated on insert
  KeyStore::Iterator& KeyStore::Iterator::operator++()
  {
    if( lo_itoff_ < keystore_.lo_fill_ )
    {
      lo_itoff_ += sizeof(uint64_t);
    }

    return *this;
  }

  // Post-increment version copies the Iterator first
  KeyStore::Iterator KeyStore::Iterator::operator++(int)
  {
    KeyStore::Iterator it_tmp(*this);

    operator++();

    return it_tmp;
  }

  KeyStore::Iterator& KeyStore::Iterator::operator--()
  {
    if( lo_itoff_ > 0 )
    {
      lo_itoff_ -= sizeof(uint64_t);
    }

    return *this;
  }

  KeyStore::Iterator KeyStore::Iterator::operator--(int)
  {
    KeyStore::Iterator it_tmp(*this);

    operator--();

    return it_tmp;
  }

  // Equal if keystore same and offset same
  bool KeyStore::Iterator::operator==(const Iterator& rhs)
  {
    return ( &keystore_ == &rhs.keystore_ &&
             lo_itoff_ == rhs.lo_itoff_ );
  }

  // Unequal if keystore different or offset different
  bool KeyStore::Iterator::operator!=(const Iterator& rhs)
  {
    return ( &keystore_ != &rhs.keystore_ ||
             lo_itoff_ != rhs.lo_itoff_ );
  }
  
  const std::pair<char*, uint64_t>& KeyStore::Iterator::operator*()
  {
    // Get current length-offset
    uint64_t lo =
      *reinterpret_cast<uint64_t *>(keystore_.buffer_base_ + lo_itoff_);

    // Unpack it
    uint64_t len = lo >> keystore_.off_bit_count_;
    uint64_t off = lo & keystore_.off_mask_;

    // Create pointer to key
    char* key =
      reinterpret_cast<char*>(keystore_.buffer_base_ + off);

    // Store pair internally so we can return a reference to it
    cur_pair_ = std::make_pair(key, len);

    return cur_pair_;
  }

  const std::pair<char*, uint64_t>* KeyStore::Iterator::operator->()
  {
    return &(this->operator*());
  }

  // ---- Sorter ----

  KeyStore::Sorter::Sorter(KeyStore& keystore)
    : keystore_(keystore)
  { }

  bool KeyStore::Sorter::operator()(const uint64_t& a, const uint64_t& b)
  {
    // Unpack lengths, offsets
    uint64_t len_a = a >> keystore_.off_bit_count_;
    uint64_t len_b = b >> keystore_.off_bit_count_;

    uint64_t off_a = a & keystore_.off_mask_;
    uint64_t off_b = b & keystore_.off_mask_;

    // Use locale-dependent comparison
    if( keystore_.loc_ )
    {
      if( keystore_.coll_->compare(keystore_.buffer_base_ + off_a,
                                   keystore_.buffer_base_ + off_a + len_a,
                                   keystore_.buffer_base_ + off_b,
                                   keystore_.buffer_base_ + off_b + len_b)
            == -1 )
      {
        return true;
      }

      return false;
    }

    // Use byte comparison (much faster)
    int comp = memcmp(keystore_.buffer_base_ + off_a,
                      keystore_.buffer_base_ + off_b,
                      (len_a < len_b) ? len_a : len_b);


    if( comp < 0 || ( comp == 0 && len_a < len_b ) )
    {
      return true;
    }

    return false;
  }
}
