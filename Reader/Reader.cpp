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

#include "Reader.hpp"

#include <cstring>
#include <stdexcept>

namespace Fort
{
  // ---- Constructors / destructors ----

  Reader::Pushback::Pushback(size_t size)
    : size_(size), fill_(0)
  {
    buffer_ = new char[size_];
  }

  Reader::Pushback::~Pushback()
  {
    delete[] buffer_;
  }

  Reader::~Reader()
  { }

  // ---- Public member functions ----

  void Reader::Pushback::push(char* base, size_t size)
  {
    if( size > size_ )
    {
      throw( std::runtime_error("Pushback buffer too small.") );
    }

    std::memcpy(buffer_, base, size);
    fill_ = size;
  }

  size_t Reader::Pushback::pop(char* base, size_t max_size)
  {
    if( max_size < fill_ )
    {
      throw( std::runtime_error("Reader buffer too small for pushback.") );
    }

    std::memcpy(base, buffer_, fill_);

    size_t tmp_fill = fill_;
    fill_ = 0;

    return tmp_fill;
  }

}
