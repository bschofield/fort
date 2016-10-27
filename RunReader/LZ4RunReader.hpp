//
// fort: LZ4-compressed run reader
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

#include "lz4.h"
#include "lz4frame.h"

#include "RingBuffer.hpp"
#include "RunReader.hpp"

namespace Fort
{
  class LZ4RunReader : public RunReader
  {
    public:

      LZ4RunReader(const std::string& run_file,
                   const size_t buffer_size,
                   const double trigger_fraction = DEFAULT_TRIGGER_FRACTION);

      ~LZ4RunReader();

      // Avoid defaults
      LZ4RunReader(const LZ4RunReader& other) = delete;
      LZ4RunReader& operator=(const LZ4RunReader& other) = delete;

      // Returns the address and length of the next element.
      // No more elements indicated by returning <nullptr, 0>
      std::pair<char*, size_t> next();

    private:

      // Keep reading until decompressed buffer 90% full
      static constexpr double DEFAULT_TRIGGER_FRACTION = 0.9;

      // Structure for poll()
      struct pollfd fds_[1];

      // Ring-buffer for compressed data
      RingBuffer comp_;

      // Ring-buffer for uncompressed result
      RingBuffer decomp_;

      // Hit EOF?
      bool eof_;

      // Fill trigger point for processing
      size_t trigger_;

      // LZ4 decompression context
      LZ4F_decompressionContext_t lz4_;

      // Get size of next key in buffer
      size_t next_size() const;

      // Decompress data from one buffer to another
      void decompress();

  };
}
