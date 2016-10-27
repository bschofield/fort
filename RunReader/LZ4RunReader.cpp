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

#include <algorithm>
#include <cstring>
#include <stdexcept>

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "LZ4RunReader.hpp"
#include "Log.hpp"

namespace Fort
{
  // ---- Constructors / destructors ----

  LZ4RunReader::LZ4RunReader(const std::string& run_file,
                             const size_t buffer_size,
                             const double trigger_fraction)
    : comp_(buffer_size), decomp_(buffer_size), eof_(false),
      trigger_(std::max(trigger_fraction, 1.0) * buffer_size)
  {
    // Check that the trigger size leaves us at least space to extract
    // a length from the buffer
    if( trigger_ < sizeof(size_t) )
    {
      throw std::runtime_error("Buffer trigger size too small");
    }

    // Open the input file
    int fd = open(run_file.c_str(), O_RDONLY);

    if( fd == -1 )
    {
      throw std::runtime_error("Error opening run file " + run_file + " : "
                                 + strerror(errno));
    }

    // Set up poll() structure
    fds_[0].fd = fd;
    fds_[0].events = POLLIN;

    // Set fd as nonblocking
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);

    // Create LZ4 decompression context
    LZ4F_errorCode_t r = LZ4F_createDecompressionContext(&lz4_, LZ4F_VERSION);

    if (LZ4F_isError(r))
    {
      throw std::runtime_error("Error creating LZ4 compression context.");
    }
  }

  LZ4RunReader::~LZ4RunReader()
  {
    // Delete decompression context
    LZ4F_freeDecompressionContext(lz4_);
  }

  // ---- Public member functions ----

  std::pair<char*, size_t> LZ4RunReader::next()
  {
    // Is there a complete key in the buffer?
    if( decomp_.fill() >= sizeof(size_t) &&
        decomp_.fill() >= (sizeof(size_t) + next_size()) )
    {
      auto ret = std::make_pair(decomp_.base() + decomp_.lo() + sizeof(size_t),
                                next_size());

      decomp_.advance_lo(ret.second + sizeof(size_t));

      return ret;
    }
    // No key. Did we hit eof?
    else if( eof_ )
    {
      if( decomp_.fill() )
      {
        WARNING("Run file had " << decomp_.fill() << " extraneous bytes at end");
      }

      if( comp_.fill() )
      {
        WARNING("Run file consumed with " << comp_.fill() << " compressed"
                " bytes remaining");
      }

      return std::pair<char*, size_t>(nullptr, 0);
    }
    // We need some more data
    else
    {
      // Fill buffer until we have hit the trigger point, and we have a
      // a complete key
      while( !eof_ &&
             ( decomp_.fill() < trigger_ || 
               decomp_.fill() < (sizeof(size_t) + next_size()) ) )
      {
        // First try to decompress that which we have
        if( comp_.fill() )
        {
          decompress();

          // Skip read if we got enough data
          if( decomp_.fill() >= trigger_ && 
              decomp_.fill() >= (sizeof(size_t) + next_size()) )
          {
            continue;
          }
        }

        if( poll(fds_, 1, -1) < 0 )
        {
          WARNING("poll() failed, input may have terminated prematurely.");
          eof_ = true;
        }

        // Read into the compressed buffer
        int bytes_read = read(fds_[0].fd, comp_.base() + comp_.hi(),
                                          comp_.size() - comp_.fill());

        if( bytes_read <= 0 )
        {
          eof_ = true;

          if( bytes_read < 0 )
          {
            WARNING("read() failed, input may have terminated prematurely.");
          }
        }
        else
        {
          comp_.advance_hi(bytes_read);
          decompress();
        }
      }

      // Warn if eof without a complete key
      if( eof_ && ( decomp_.fill() < sizeof(size_t) ||
                    decomp_.fill() < (sizeof(size_t) + next_size()) ) )
      {
        WARNING("Run file had " << decomp_.fill() << " extraneous bytes at end");
        return std::pair<char*, size_t>(nullptr, 0);
      }
      
      // We now have at least one key in the buffer. Return the first.
      auto ret = std::make_pair(decomp_.base() + decomp_.lo() + sizeof(size_t),
                                next_size());

      decomp_.advance_lo(ret.second + sizeof(size_t));

      return ret;
    }
  }

  // ---- Private member functions ----

  uint64_t LZ4RunReader::next_size() const
  {
    return static_cast<uint64_t>( *(decomp_.base() + decomp_.lo()) );
  }

  void LZ4RunReader::decompress()
  {
    // Number of compressed bytes available
    size_t comp_len = comp_.fill();

    // Space available for decompression
    size_t decomp_len = decomp_.size() - decomp_.fill();

    // Attempt the decompression
    size_t n = LZ4F_decompress(lz4_,
                               decomp_.base() + decomp_.hi(), &decomp_len,
                               comp_.base() + comp_.lo(), &comp_len,
                               NULL);

    if( LZ4F_isError(n) )
    {
      throw std::runtime_error("Error during LZ4 decompression.");
    }

    // Update ring buffer indicators
    comp_.advance_lo(comp_len);
    decomp_.advance_hi(decomp_len);

    return;
  }

}
