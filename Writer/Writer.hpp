//
// fort: Base class for data-writers
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

#include <cstdint>

namespace Fort
{
  class Writer
  {
    public:

      // Force destructor calls to be dispatched to the derived class
      virtual ~Writer();

      // Returns true if more data to read, false otherwise
      virtual void write(const char* key, const std::size_t key_len) = 0;

      // Called when the stream is done
      virtual void end() = 0;
  };
}
