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

#pragma once

#include <array>
#include <iostream>
#include <mutex>
#include <string>
#include <sstream>

namespace Fort
{
  class Log
  {
    public:

      // Allowable levels
      enum Level
      {
        DEBUG = 0,
        INFO = 1,
        WARNING = 2,
        FATAL = 3
      };

      // Initialise logging
      void init(Level min_level = Level::INFO,
                std::ostream& out = std::cerr);

      // Return an instance of the singleton
      static Log& instance();

      // Report a message
      void message(Level level, std::string message,
                   const char* function);

    private:

      // Private constructor
      Log();

      // No copying
      Log(const Log& other) = delete;
      Log& operator=(const Log& other) = delete;

      // Textual strings for levels
      const std::array<std::string, 4> LEVEL_STRINGS
        = { { "DEBUG", "INFO", "WARNING", "FATAL" } };

      // Minimum level to report
      Level min_level_;

      // Stream to log to
      std::ostream* out_;

      // Thread safety
      std::mutex mutex_;
  };
}

// Helper macro, specified level
#define LOG(level, data) Fort::Log::instance().message(level, \
                           static_cast<std::ostringstream&> \
                             (std::ostringstream().flush() << data).str(), \
                           __func__)

// Helper macros, implicit levels
#ifdef BUILD_DEBUG
#define DEBUG(data)   LOG(Fort::Log::Level::DEBUG, data)
#else
#define DEBUG(data)   ((void) 0);
#endif

#define INFO(data)    LOG(Fort::Log::Level::INFO, data)
#define WARNING(data) LOG(Fort::Log::Level::WARNING, data)
#define FATAL(data)   LOG(Fort::Log::Level::FATAL, data)
