//
// fort: I/O Synchronisation
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

#include "SyncIO.hpp"

#include "Log.hpp"

namespace Fort
{
  SyncIO::SyncIO(int readers, int writers, int total)
    : using_total_(total ? true : false),
      available_{ {total, readers, writers} },
      wakeups_{ {0, 0, 0} }
  { }

  void SyncIO::acquire(int type)
  {
    // Always grab one from the total first, if we're using one
    if( type != TOTAL && using_total_ )
    {
      this->acquire(TOTAL);
    }

    // Acquire lock in this scope
    std::unique_lock<std::mutex> lock(mutex_[type]);

    // Claim an appropriate number of instances of the resource
    available_[type]--;

    // Do we need to block until resource becomes available?
    if( available_[type] < 0 )
    {
      // Wait until woken; if woken check that it is non-spurious (i.e.
      // there are a positive number of unclaimed genuine wake-ups) and
      // re-wait otherwise
      cond_[type].wait(lock, [&]() { return (wakeups_[type] > 0); });

      // Genuine wakeup acknowledged
      wakeups_[type]--;
    }

    return;
  }

  void SyncIO::release(int type)
  {
    // Release one from the total, if we're using one
    if( type != TOTAL && using_total_ )
    {
      this->release(TOTAL);
    }

    // Acquire lock in this scope
    std::unique_lock<std::mutex> lock(mutex_[type]);

    // Release one unit of resource
    available_[type]++;

    // Anyone waiting?
    if( available_[type] <= 0 )
    {
      // We are about to generate a genuine wake-up
      wakeups_[type]++;

      // Notify any thread of the appropriate type
      cond_[type].notify_one();
    }

    return;
  }

}
