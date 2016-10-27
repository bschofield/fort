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

#pragma once

#include <array>
#include <mutex>
#include <condition_variable>

namespace Fort
{
  class SyncIO
  {
    public:

      // Reader/writer constants
      static const int READER = 1;
      static const int WRITER = 2;

      // Constructor defaults to not using a total
      SyncIO(int readers, int writers, int total = 0);

      // Claim an instance
      void acquire(int type);

      // Give up an instance
      void release(int type);

    private:

      // TOTAL constant is private
      static const int TOTAL = 0;

      // Flag indicating use of the total count
      const bool using_total_;

      // Available resource counts
      std::array<int, 3> available_;

      // Count of genuine wake-ups not yet acknowledged
      std::array<int, 3> wakeups_;

      // Locking mutex for acq/rel operations (of any type)
      std::array<std::mutex, 3> mutex_;

      // Condition variables for waits
      std::array<std::condition_variable, 3> cond_;

  };
}
