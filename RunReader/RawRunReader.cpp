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

#include <algorithm>
#include <cstring>
#include <stdexcept>

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "RawRunReader.hpp"
#include "Log.hpp"

namespace Fort
{
  // ---- Constructors / destructors ----

  RawRunReader::RawRunReader(const std::string& run_file,
                             const size_t buffer_size,
                             const double trigger_fraction)
    : rb_(buffer_size), eof_(false),
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
  }

  // ---- Public member functions ----

  std::pair<char*, size_t> RawRunReader::next()
  {
    // Is there a complete key in the buffer?
    if( rb_.fill() >= sizeof(size_t) &&
        rb_.fill() >= (sizeof(size_t) + next_size()) )
    {
      auto ret = std::make_pair(rb_.base() + rb_.lo() + sizeof(size_t),
                                next_size());

      rb_.advance_lo(ret.second + sizeof(size_t));

      return ret;
    }
    // No key. Did we hit eof?
    else if( eof_ )
    {
      if( rb_.fill() )
      {
        WARNING("Run file had " << rb_.fill() << " extraneous bytes at end");
      }

      return std::pair<char*, size_t>(nullptr, 0);
    }
    // We need some more data
    else
    {
      // Fill buffer until we have hit the trigger point, and we have a
      // a complete key
      while( !eof_ &&
             ( rb_.fill() < trigger_ || 
               rb_.fill() < (sizeof(size_t) + next_size()) ) )
      {
        if( poll(fds_, 1, -1) < 0 )
        {
          WARNING("poll() failed, input may have terminated prematurely.");
          eof_ = true;
        }

        int bytes_read = read(fds_[0].fd, rb_.base() + rb_.hi(),
                                          rb_.size() - rb_.fill());

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
          rb_.advance_hi(bytes_read);
        }
      }

      // Warn if eof without a complete key
      if( eof_ && ( rb_.fill() < sizeof(size_t) ||
                    rb_.fill() < (sizeof(size_t) + next_size()) ) )
      {
        WARNING("Run file had " << rb_.fill() << " extraneous bytes at end");
        return std::pair<char*, size_t>(nullptr, 0);
      }
      
      // We now have at least one key in the buffer. Return the first.
      auto ret = std::make_pair(rb_.base() + rb_.lo() + sizeof(size_t),
                                next_size());

      rb_.advance_lo(ret.second + sizeof(size_t));

      return ret;
    }
  }

  // ---- Private member functions ----

  uint64_t RawRunReader::next_size() const
  {
    return static_cast<uint64_t>( *(rb_.base() + rb_.lo()) );
  }

}
