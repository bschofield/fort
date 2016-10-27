//
// fort: Simple logging
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

#include <cstring>
#include <ctime>
#include <thread>

#include "Log.hpp"

namespace Fort
{
  // ---- Constructors/destructors ----

  Log::Log()
  { }

  // ---- Public member functions ----

  void Log::init(Log::Level min_level, std::ostream& out)
  {
    min_level_ = min_level;
    out_ = &out;
  }

  // Returns an instance of the singleton
  Log& Log::instance()
  {
    // This is where the Log object really lives
    static Log instance;

    return instance;
  }

  void Log::message(Log::Level level, std::string message,
                    const char* function)
  {
    // Lock this scope
    std::unique_lock<std::mutex> lock(mutex_);

    // Get time and date
    time_t epoch;
    char* date;

    time(&epoch);
    date = asctime(gmtime(&epoch));
    date[strlen(date)-1] = 0;

    // Report log message
    *out_ << std::dec << epoch << " " << function
          << "() : " << LEVEL_STRINGS[level] << " : "
          << message << std::endl;
  }

}
