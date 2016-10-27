//
// fort: Sorted run creator
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

#include <iostream>
#include <sstream>

#include "RunCreator.hpp"

#include "Log.hpp"

namespace Fort
{
  // ---- Constructors/destructors ----

  RunCreator::RunCreator(const unsigned int creator_id,
                         const std::string& runs_dir,
                         const size_t size, const char* locale_name,
                         SyncIO& sync_io,
                         Reader& reader, Reader::Pushback& pushback,
                         RunWriter& writer)
    : creator_id_(creator_id),
      runs_dir_(runs_dir),
      keystore_(size, locale_name),
      sync_io_(sync_io),
      reader_(reader),
      pushback_(pushback),
      writer_(writer)
  { }

  RunCreator::RunCreator(RunCreator&& other)
    : creator_id_(other.creator_id_),
      runs_dir_(std::move(other.runs_dir_)),
      keystore_(std::move(other.keystore_)),
      sync_io_(other.sync_io_),
      reader_(other.reader_),
      pushback_(other.pushback_),
      writer_(other.writer_)
  { }

  // ---- Public member functions ----

  std::vector<std::string> RunCreator::create_runs()
  {
    // Vector of files created
    std::vector<std::string> runs;

    // Loop as long as there is more data to read
    bool more_data = true;

    while( more_data )
    {
      // Empty the keystore
      keystore_.clear();

      // Read into keystore
      sync_io_.acquire(SyncIO::READER);
      more_data = reader_.read(keystore_, pushback_);
      sync_io_.release(SyncIO::READER);

      // Did the store receive any data?
      if( ! keystore_.empty() )
      {
        // Sort keystore
        keystore_.sort();

        // Acquire write lock
        sync_io_.acquire(SyncIO::WRITER);

        // Name for run file
        std::stringstream run_name;

        run_name << runs_dir_
                 << "/fort_run." << creator_id_ << "." << runs.size();

        // Write to the file
        writer_.write(keystore_, run_name.str());

        // Add to the list of files written by this creator
        runs.push_back(run_name.str());

        // Close file and release write lock
        sync_io_.release(SyncIO::WRITER);
      }
    }

    return runs;

  }

}
