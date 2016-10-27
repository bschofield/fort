//
// fort: Newline-delimited key writer
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

#include <algorithm>
#include <cstring>
#include <stdexcept>

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include "TextWriter.hpp"
#include "Log.hpp"

namespace Fort
{
  // ---- Constructors / destructors ----

  TextWriter::TextWriter(int fd, size_t buffer_size)
    : fd_(fd), buffer_size_(buffer_size), fill_(0)
  {
    // Initialise write buffer
    buffer_ = new char[buffer_size_];
  }

  TextWriter::~TextWriter()
  {
    delete[] buffer_;
  }

  // ---- Public member functions ----

  void TextWriter::write(const char* key, size_t key_len)
  {
    // Will this key fit in the buffer?
    if( (key_len + 1) <= (buffer_size_ - fill_) )
    {
      // Add it and a newline
      memcpy(buffer_ + fill_, key, key_len);
      buffer_[fill_ + key_len] = '\n';

      // Move on
      fill_ += key_len + 1;
    }
    else
    {
      // Flush anything in the buffer
      if( fill_ )
      {
        ::write(fd_, buffer_, fill_);
        fill_ = 0;
      }

      // Will the key fit now?
      if( (key_len + 1) <= (buffer_size_ - fill_) )
      {
        // Add it and a newline
        memcpy(buffer_, key, key_len);
        buffer_[key_len] = '\n';

        // Move on
        fill_ = key_len + 1;
      }
      else
      {
        // Straight out
        ::write(fd_, key, key_len);
        ::write(fd_, "\n", 1);
      }
    }

    return;
  }

  void TextWriter::end()
  {
    // Flush anything in the buffer
    if( fill_ )
    {
      ::write(fd_, buffer_, fill_);
      fill_ = 0;
    }

    return;
  }

}
