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

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <sys/mman.h>
#include <unistd.h>

#include "RingBuffer.hpp"

namespace Fort
{
  // ---- Constructors/destructors ----

  RingBuffer::RingBuffer(size_t req_size)
    : lo_(0), hi_(0)
  {
    // Ensure we are requesting a multiple of the system page size
    size_t page_size = sysconf(_SC_PAGESIZE);

    if( req_size % page_size )
    {
      req_size += page_size - (req_size % page_size);
    }

    size_ = req_size;

    // Create a temporary file to back our memory-mapped region
    char tmp_file[22];
    int fd;

    strcpy(tmp_file, "/tmp/fort.mmap.XXXXXX");
    fd = mkstemp(const_cast<char*>(tmp_file));

    if( fd < 0 )
    {
      throw( std::runtime_error("Failed to create temporary file") );
    }

    // Schedule for deletion when fd closes
    if( unlink(tmp_file) < 0 )
    {
      throw( std::runtime_error("Failed to unlink temporary file") );
    }

    // Set size of ring buffer
    if( ftruncate(fd, size_) )
    {
      throw( std::runtime_error("Failed to set size of temporary file") );
    }

    // Reserve an area of memory suitable for both images of the buffer
    buf_ = static_cast<char*>(mmap(NULL, 2 * size_, PROT_NONE,
                                   MAP_ANONYMOUS | MAP_PRIVATE, -1, 0));

    if( buf_ == MAP_FAILED )
    {
      throw( std::runtime_error("Failed to memory-map buffer") );
    }

    // Map the first image, backed by the fd
    void* addr = mmap(buf_, size_, PROT_READ | PROT_WRITE,
                      MAP_FIXED | MAP_SHARED, fd, 0);

    if( addr != buf_ )
    {
      throw( std::runtime_error("Failed to memory-map first buffer image") );
    }

    // Map the second image onto the same physical region as the first
    addr = mmap(buf_ + size_, size_, PROT_READ | PROT_WRITE,
                MAP_FIXED | MAP_SHARED, fd, 0);

    if( addr != buf_ + size_ )
    {
      throw( std::runtime_error("Failed to memory-map first buffer image") );
    }
  }

  RingBuffer::~RingBuffer()
  {
    // Free the original ring-buffer mapping
    munmap(buf_, 2 * size_);
  }

  // ---- Public member functions ----

  char* RingBuffer::base() const
  {
    return buf_;
  }

  size_t RingBuffer::size() const
  {
    return size_;
  }

  size_t RingBuffer::lo() const
  {
    return lo_;
  }

  size_t RingBuffer::hi() const
  {
    return hi_;
  }

  size_t RingBuffer::fill() const
  {
    // Both pointers wrapped the same number of times
    if( hi_ >= lo_ )
    {
      return (hi_ - lo_);
    }

    // High pointer has wrapped but not low
    return (size_ - lo_ + hi_);
  } 

  void RingBuffer::lo(size_t new_lo)
  {
    lo_ = new_lo;

    return;
  }

  void RingBuffer::hi(size_t new_hi)
  {
    hi_ = new_hi;

    return;
  }

  void RingBuffer::advance_lo(size_t delta)
  {
    lo_ += delta;

    if( lo_ > size_ )
    {
      lo_ -= size_;
    }

    return;
  }

  void RingBuffer::advance_hi(size_t delta)
  {
    hi_ += delta;

    if( hi_ > size_ )
    {
      hi_ -= size_;
    }

    return;
  }

}
