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

#pragma once

#include <cstddef>
#include <poll.h>

#include "Writer.hpp"

namespace Fort
{
  class TextWriter : public Writer
  {
    public:

      TextWriter(int fd, size_t buffer_size = DEFAULT_BUFFER_SIZE);

      ~TextWriter();

      // Avoid defaults
      TextWriter(const TextWriter& other) = delete;
      TextWriter& operator=(const TextWriter& other) = delete;

      // Write a key
      void write(const char* key, size_t key_len);

      // Finish stream
      void end();

    private:

      // Default output buffer size
      static const uint64_t DEFAULT_BUFFER_SIZE = 16384;

      // Output fd
      int fd_;

      // Size of buffer
      size_t buffer_size_;

      // Fill of buffer
      size_t fill_;

      // Output buffer
      char* buffer_;
  };
}
