//
// fort: LZ4-compressed run writer
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
#include <string>

#include "lz4.h"
#include "lz4frame.h"
#include "lz4frame_static.h"

#include "RingBuffer.hpp"
#include "RunWriter.hpp"
#include "KeyStore.hpp"

namespace Fort
{
  class LZ4RunWriter : public RunWriter
  {
    public:

      // Constructor/destructor
      LZ4RunWriter(size_t max_element);
      ~LZ4RunWriter();
      
      // Write out keystore
      void write(const KeyStore& keystore, const std::string& run_file);

    private:

      // Default size of ring-buffer is 1MiB
      static constexpr size_t DEFAULT_RING_SIZE = (1 << 20);

      // LZ4 parameters
      static constexpr size_t LZ4_HEADER_SIZE = 19;
      static constexpr size_t LZ4_FOOTER_SIZE = 4;
      static constexpr LZ4F_preferences_t LZ4_PREFS =
      {
        { LZ4F_max256KB, LZ4F_blockLinked, LZ4F_noContentChecksum, LZ4F_frame,
          0, { 0, 0 } },
        0,
        0,
        { 0, 0, 0, 0 },
      };

      // Ring-buffer
      RingBuffer rb_;

      // Compressed data buffer
      char* comp_;

      // LZ4 compression context
      LZ4F_compressionContext_t lz4_;

      // Size of LZ4 frame
      size_t frame_size_;

      // Size of compressed data buffer
      size_t comp_size_;

  };
}
