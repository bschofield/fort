//
// fort: Ringbuffer using mmap()
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

namespace Fort
{
  class RingBuffer
  {
    public:

      // Constructor/destructor
      RingBuffer(size_t req_size);
      ~RingBuffer();
     
      // Get base address
      char* base() const;

      // Get size
      size_t size() const;

      // Get current low- and high-end positions
      size_t lo() const;
      size_t hi() const;
      
      // Compute current fill
      size_t fill() const;

      // Set current low- and high-end positions
      void lo(size_t new_lo);
      void hi(size_t new_hi);

      // Move forward in the ring-buffer, wrapping back if required
      void advance_lo(size_t delta);
      void advance_hi(size_t delta);

    private:

      // Ring-buffer base
      char* buf_;

      // Ring-buffer extent
      size_t size_;

      // Current low- and high-end positions in ring-buffer
      size_t lo_;
      size_t hi_;

  };
}
