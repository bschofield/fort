//
// fort: Base class for data-readers
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

#include "KeyStore.hpp"

namespace Fort
{
  class Reader
  {
    public:

      // Push-back buffer for unconsumed data at the end of a read
      class Pushback
      {
        public:

          Pushback(size_t size);
          ~Pushback();

          // Avoid default operators
          Pushback(const Pushback& other) = delete;
          Pushback& operator=(const Pushback& other) = delete;

          // Push data into Pushback
          void push(char* base, size_t size);

          // Pop data out
          size_t pop(char* base, size_t max_size);

        private:

          // Pushback buffer
          char* buffer_;

          // Pushback buffer size
          size_t size_;

          // Bytes used in buffer
          size_t fill_;
      };

      // Force destructor calls to be dispatched to the derived class
      virtual ~Reader();

      // Returns true if more data to read, false otherwise
      virtual bool read(KeyStore& keystore, Pushback& pushback) = 0;
  };
}
