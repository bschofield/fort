//
// fort: Newline-delimited key reader
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

#include "Reader.hpp"

namespace Fort
{
  class TextReader : public Reader
  {
    public:

      TextReader(int fd,
                 size_t buffer_size,
                 double trigger_fraction = DEFAULT_TRIGGER_FRACTION);

      ~TextReader();

      // Avoid defaults
      TextReader(const TextReader& other) = delete;
      TextReader& operator=(const TextReader& other) = delete;

      // Read data into a Keystore
      bool read(KeyStore& keystore, Pushback& pushback);

    private:

      // Keep reading until buffer 90% full
      static constexpr double DEFAULT_TRIGGER_FRACTION = 0.9;

      // Structure for poll()
      struct pollfd fds_[1];

      // Size of buffer
      size_t buffer_size_;

      // Fill of buffer
      size_t fill_;

      // Current index into buffer
      size_t index_;

      // Fill trigger point for processing
      size_t trigger_;

      // Read buffer
      char* buffer_;

  };
}
