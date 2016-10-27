//
// fort: Sorted run merger
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
#include <queue>
#include <string>
#include <vector>

#include "RunReader.hpp"
#include "Writer.hpp"

namespace Fort
{
  class RunMerger
  {
    public:

      RunMerger(const char* locale_name,
                const std::vector<RunReader*>& run_readers,
                Writer& writer);

      ~RunMerger();

      // No copying
      RunMerger(RunMerger& other) = delete;
      RunMerger& operator=(RunMerger& other) = delete;

      // Merges
      void merge();

      // Forward-declare Elem
      class Elem;

    private:

      // Vector of run readers
      const std::vector<RunReader*>& run_readers_;

      // Run writer
      Writer& writer_;

      // Comparison class needs to be completely declared as instantiated
      // within template definition (at least for libstdc++)
      class Sorter
      {
        public:

          // Constructor
          Sorter(const RunMerger& run_merger);

          // Comparison operator for sort
          bool operator()(const Elem& a, const Elem& b);

        private:

          // Associated run merger
          const RunMerger& run_merger_;
      };

      // Priority queue
      std::priority_queue<Elem, std::vector<Elem>, Sorter> queue_;
      
      // Locale and collation facet
      std::locale* loc_;
      std::collate<char>* coll_;
  };
     
  // Data element
  class RunMerger::Elem
  {
    public:

      // Constructor
      Elem(std::pair<char*, size_t>& key, RunReader* reader);

      // Pointer to key
      char* ptr_;

      // Length of key
      size_t len_;

      // Corresponding run reader
      RunReader* reader_;
  };

}
