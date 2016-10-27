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

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <fstream>

#include <sys/mman.h>
#include <unistd.h>

#include "LZ4RunWriter.hpp"
#include "Log.hpp"

namespace Fort
{
  // Need to provide this here to avoid undefined references error when linking
  constexpr LZ4F_preferences_t LZ4RunWriter::LZ4_PREFS;

  // ---- Constructors/destructors ----

  // The max unit we will be asked to write is the max element plus a length.
  // Use the default ring size, unless it is too small for our max element
  LZ4RunWriter::LZ4RunWriter(size_t max_element)
    : rb_(std::max(size_t(DEFAULT_RING_SIZE), max_element + sizeof(size_t)))
  {
    // Compute compressed buffer size
    frame_size_ = LZ4F_compressBound(rb_.size(), &LZ4_PREFS);
    comp_size_ =  frame_size_ + LZ4_HEADER_SIZE + LZ4_FOOTER_SIZE;

    // Create an output buffer
    comp_ = new char[comp_size_];

    // Create LZ4 compression context
    LZ4F_errorCode_t r = LZ4F_createCompressionContext(&lz4_, LZ4F_VERSION);

    if (LZ4F_isError(r))
    {
      throw std::runtime_error("Error creating LZ4 compression context.");
    }
  }

  LZ4RunWriter::~LZ4RunWriter()
  {
    // Free the output buffer
    delete[] comp_;

    // Delete compression context
    LZ4F_freeCompressionContext(lz4_);
  }

  // ---- Public member functions ----

  void LZ4RunWriter::write(const KeyStore& keystore,
                           const std::string& run_file)
  {
    // Open output file
    std::ofstream out(run_file, std::ios::binary);

    // Begin compression
    size_t comp_fill =
      LZ4F_compressBegin(lz4_, comp_, comp_size_, &LZ4_PREFS);

    if( LZ4F_isError(comp_fill) )
    {
      throw std::runtime_error("Error beginning LZ4 compression.");
    }

    // Iterate through keystore
    size_t n;

    for( auto& kv : keystore )
    {
      // Pack length into uncompressed buffer
      size_t tmp = kv.second;
      char* addr = rb_.base() + rb_.hi();

      for( uint_fast8_t i = 0; i < sizeof(size_t); ++i )
      {
        addr[i] = tmp & 0xff;
        tmp = tmp >> 8;
      }

      // Copy data into uncompressed buffer
      memcpy(addr + sizeof(size_t), kv.first, kv.second);

      // Compress the data
      n = LZ4F_compressUpdate(lz4_,
                              comp_ + comp_fill, comp_size_ - comp_fill,
                              addr, kv.second + sizeof(size_t), NULL);

      if( LZ4F_isError(n) )
      {
        throw std::runtime_error("Error during LZ4 compression.");
      }

      comp_fill += n;

      // If remaining space would not fit a frame, write
      if( (comp_size_ - comp_fill) < (frame_size_ + LZ4_FOOTER_SIZE) )
      {
        // Write out compressed buffer
        out.write(comp_, comp_fill);
        comp_fill = 0;
      }
    }

    // Finish off compression
    n = LZ4F_compressEnd(lz4_,
                         comp_ + comp_fill, comp_size_ - comp_fill,
                         NULL);

    if( LZ4F_isError(n) )
    {
      throw std::runtime_error("Error finishing LZ4 compression.");
    }

    comp_fill += n;

    // Write out final compressed buffer
    out.write(comp_, comp_fill);

    return;
  }
}
