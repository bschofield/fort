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

#include <algorithm>
#include <cstring>
#include <stdexcept>

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include "TextReader.hpp"
#include "Log.hpp"

namespace Fort
{
  // ---- Constructors / destructors ----

  TextReader::TextReader(int fd, size_t buffer_size, double trigger_fraction)
    : buffer_size_(buffer_size), fill_(0), index_(0)
  {
    // Set up poll() structure
    fds_[0].fd = fd;
    fds_[0].events = POLLIN;

    // Set fd as nonblocking
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);

    // Initialise read buffer
    buffer_ = new char[buffer_size_];

    // Set processing trigger point for partial reads
    trigger_ = std::max(trigger_fraction, 1.0) * buffer_size_;
  }

  TextReader::~TextReader()
  {
    delete[] buffer_;
  }

  // ---- Public member functions ----

  bool TextReader::read(KeyStore& keystore, Pushback& pushback)
  {
    // -- Prime with pushback --

    index_ = 0;
    fill_ = pushback.pop(buffer_, buffer_size_);

    // -- Loop until stream ends or KeyStore full --

    while( 1 )
    {
      // -- Read into buffer --

      bool eof = false;

      while( !eof && fill_ < trigger_ )
      {
        if( poll(fds_, 1, -1) < 0 )
        {
          // Warn, but build anyway
          WARNING("poll() failed, input may have terminated prematurely.");
          eof = true;
        }

        int bytes_read = ::read(fds_[0].fd, buffer_ + fill_,
                                            buffer_size_ - fill_);

        if( bytes_read <= 0 )
        {
          eof = true;

          if( bytes_read < 0 )
          {
            // Warn, but build anyway
            WARNING("read() failed, input may have terminated prematurely.");
          }
        }
        else
        {
          fill_ += bytes_read;
        }
      }

      // -- Insert into keystore --

      // Loop over records in buffer
      while( 1 )
      {
        size_t i = index_;

        // Make i index of next newline, or end of buffer
        while( i < fill_ && buffer_[i] != '\n' )
        {
          i++;
        }

        // Consumed buffer?
        if( i == fill_ )
        {
          // Move end section back to start
          std::memmove(buffer_, buffer_ + index_, fill_ - index_);
          fill_ = fill_ - index_;
          index_ = 0;

          // All done? (Will ignore last line if no newline)
          if( eof )
          {
            return false;
          }

          // Go for next buffer read
          break;
        }

        // Do insert
        switch( keystore.insert(buffer_ + index_, i - index_) )
        {
          // Key too long
          case KeyStore::KeyTooLong:

            throw( std::runtime_error("Key too long when inserting") );

          // Full
          case KeyStore::NotEnoughSpace:

            pushback.push(buffer_ + index_, fill_ - index_);

            return true;
            
          // Succcess
          default:

            ;
        }
         
        // Move on to next record in buffer, skipping newline
        index_ = i + 1;
      }

    }

  }

}
