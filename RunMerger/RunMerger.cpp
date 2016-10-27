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

#include "RunMerger.hpp"
#include "Log.hpp"

#include <algorithm>
#include <cstring>

namespace Fort
{
  // ---- Constructors/destructors ----

  RunMerger::RunMerger(const char* locale_name,
                       const std::vector<RunReader*>& run_readers,
                       Writer& writer)
    : run_readers_(run_readers), writer_(writer), queue_(*this)
  {
    // If specified, set locale for sort
    if( locale_name )
    {
      loc_ = new std::locale(locale_name);
      coll_ = const_cast<std::collate<char>*>
                ( &std::use_facet< std::collate<char> >(*loc_) );
    }
    else
    {
      // Locale not to be used
      loc_ = nullptr;
      coll_ = nullptr;
    }
  }

  RunMerger::~RunMerger()
  {
    if( loc_ )
    {
      delete loc_;
    }
  }

  // ---- Public member functions ----

  void RunMerger::merge()
  {
    // Prime queue
    for( RunReader* reader : run_readers_ )
    {
      auto next = reader->next();

      if( next.first )
      {
        queue_.emplace(next, reader);
      }
    }

    // While queue is populated...
    while( ! queue_.empty() )
    {
      // Get the next element and write it
      Elem top = queue_.top();
      writer_.write(top.ptr_, top.len_);

      // Replace with another element from the same queue
      queue_.pop();
      auto next = top.reader_->next();

      if( next.first )
      {
        queue_.emplace(next, top.reader_);
      }
    }

    // Done
    writer_.end();

    return;
  }

  // ---- Element ----

  RunMerger::Elem::Elem(std::pair<char*, size_t>& key, RunReader* reader)
    : ptr_(key.first), len_(key.second), reader_(reader)
  { }

  // ---- Sorter ----

  RunMerger::Sorter::Sorter(const RunMerger& run_merger)
    : run_merger_(run_merger)
  { }

  bool RunMerger::Sorter::operator()(const Elem& a, const Elem& b)
  {
    // Use locale-dependent comparison?
    if( run_merger_.loc_ )
    {
      if( run_merger_.coll_->compare(b.ptr_, b.ptr_ + b.len_,
                                     a.ptr_, a.ptr_ + a.len_) == -1 )
      {
        return true;
      }

      return false;
    }

    // Use byte comparison (much faster)
    int comp = memcmp(b.ptr_, a.ptr_, (b.len_ < a.len_) ? b.len_ : a.len_);

    if( comp < 0 || ( comp == 0 && b.len_ < a.len_ ) )
    {
      return true;
    }

    return false;
  }

}
