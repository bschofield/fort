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

#pragma once

#include <cstdint>
#include <iterator>
#include <locale>
#include <string>
#include <utility>

namespace Fort
{
  class KeyStore
  {
    public:

      // Nested class
      class Iterator;

      // Return
      enum ReturnCode
      {
        Inserted,
        NotEnoughSpace,
        KeyTooLong
      };

      // Constructor/destructor
      KeyStore(uint64_t size, const char* locale_name);
      ~KeyStore();

      // No copying
      KeyStore(KeyStore& other) = delete;
      KeyStore& operator=(KeyStore& other) = delete;

      // Allow a move
      KeyStore(KeyStore&& other);

      // Get max allowable length of a key
      uint64_t max_key_len() const;

      // Get space remaining in store for next key
      uint64_t key_space() const;

      // Test whether the store is empty
      bool empty() const;

      // Iterator start/end
      const KeyStore::Iterator begin() const;
      const KeyStore::Iterator end() const;

      // Insert a new key into the store
      ReturnCode insert(std::string& key);
      ReturnCode insert(const char* key, uint64_t key_len);

      // Sort the keys in the store
      void sort();

      // Clear the store
      void clear();

    private:

      // Buffer base and size
      char* buffer_base_;
      uint64_t buffer_size_;

      // Number of bytes used by length-offset entries
      uint64_t lo_fill_;

      // Offset to key section in buffer
      uint64_t key_off_;

      // Count of length bits in each l-o
      uint64_t off_bit_count_;

      // Mask for offset in each l-o
      uint64_t off_mask_;

      // Max length of a key in this store
      uint64_t max_key_len_;

      // Locale and collation facet
      std::locale* loc_;
      std::collate<char>* coll_;

      // Comparison class
      class Sorter
      {
        public:

          // Constructor
          Sorter(KeyStore& keystore);

          // Comparison operator for sort
          bool operator()(const uint64_t& a, const uint64_t& b);

        private:

          // Associated keystore
          const KeyStore& keystore_;
      };

  };

  // Bidirectional Iterator over immutable pairs of (value, length) objects
  class KeyStore::Iterator
    : public std::iterator< std::bidirectional_iterator_tag,
                            std::pair<const char* const, const uint64_t> >
                                
  {
    public:

      // Constructor
      Iterator(const KeyStore& keystore, uint64_t lo_itoff);

      // Copy constructor
      Iterator(const Iterator& it);

      // pre-increment, post-increment (int is a dummy)
      Iterator& operator++();
      Iterator operator++(int); 

      Iterator& operator--();
      Iterator operator--(int);

      bool operator==(const Iterator& rhs);
      bool operator!=(const Iterator& rhs);

      const std::pair<char*, uint64_t>& operator*();
      const std::pair<char*, uint64_t>* operator->();

    private:

      // The KeyStore within which we are iterating
      const KeyStore& keystore_;

      // Current Iterator offset within length-offset table
      uint64_t lo_itoff_;

      // Pair to be returned
      std::pair<char*, uint64_t> cur_pair_;
  };

}
