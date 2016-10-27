//
// fort: Raw (uncompressed) run reader
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

#include "RingBuffer.hpp"
#include "RunReader.hpp"

namespace Fort
{
  class RawRunReader : public RunReader
  {
    public:

      RawRunReader(const std::string& run_file,
                   const size_t buffer_size,
                   const double trigger_fraction = DEFAULT_TRIGGER_FRACTION);

      // Avoid defaults
      RawRunReader(const RawRunReader& other) = delete;
      RawRunReader& operator=(const RawRunReader& other) = delete;

      // Returns the address and length of the next element.
      // No more elements indicated by returning <nullptr, 0>
      std::pair<char*, size_t> next();

    private:

      // Keep reading until buffer 90% full
      static constexpr double DEFAULT_TRIGGER_FRACTION = 0.9;

      // Structure for poll()
      struct pollfd fds_[1];

      // Ring-buffer
      RingBuffer rb_;

      // Hit EOF?
      bool eof_;

      // Fill trigger point for processing
      size_t trigger_;

      // Get size of next key in buffer
      size_t next_size() const;

  };
}
