//
// fort: Main program
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

#include "Log/Log.hpp"
#include "RunCreator/RunCreator.hpp"
#include "SyncIO/SyncIO.hpp"
#include "Reader/TextReader.hpp"
#include "RunMerger/RunMerger.hpp"
#include "RunReader/LZ4RunReader.hpp"
#include "RunReader/RawRunReader.hpp"
#include "RunWriter/LZ4RunWriter.hpp"
#include "RunWriter/RawRunWriter.hpp"
#include "Writer/TextWriter.hpp"

#include <cstring>
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <string>
#include <future>
#include <vector>

#include <unistd.h>

bool parse_args(const int argc, const char* const argv[], size_t& mem_size,
                unsigned int& parallel, unsigned int& max_run_writers,
                unsigned int& max_run_io, std::string& tmp_dir,
                size_t& max_element, std::string& locale_string,
                bool& compress);

size_t parse_size(std::istringstream& val, size_t free_memory);

size_t measure_free_memory();

int main(const int argc, const char* const argv[])
{
  // ---- Initialisation ----

  // Initialise logging
  Fort::Log::instance().init();

  // Command-line configurable parameters
  size_t mem_size;
  unsigned int parallel;
  unsigned int max_run_writers;
  unsigned int max_run_io;
  std::string tmp_dir;
  size_t max_element;
  std::string locale_string;
  bool compress;

  // Get command-line options or set defaults
  if( ! parse_args(argc, argv, mem_size, parallel, max_run_writers,
                   max_run_io, tmp_dir, max_element, locale_string, compress) )
  {
    exit(EXIT_FAILURE);
  }

  // Warn that explicitly using the C locale is slow
  if( locale_string == "C" )
  {
    WARNING("Specifying the C locale is functionality equivalent to not "
            "specifying a locale at all, yet much slower.\n");
  }

  // Locale string for sorting
  const char* locale_name = nullptr;

  if( locale_string != "" )
  {
    locale_name = locale_string.c_str();
  }

  // Size of RAM allocated to each sorter: 
  size_t sorter_mem = (mem_size - 2*max_element) / parallel;

  // Vector of run filenames
  std::vector<std::string> run_files;

  // ---- Create runs ----

  {
    // I/O synchronizer: cannot have more than one simultaneous reader here
    Fort::SyncIO create_sync(1, max_run_writers, max_run_io);

    // Reader
    Fort::Reader::Pushback pushback(max_element);
    Fort::TextReader text_reader(STDIN_FILENO, max_element);

    // Run writer
    Fort::RunWriter* run_writer;

    if( compress )
    {
      run_writer = new Fort::LZ4RunWriter(max_element);
    }
    else
    {
      run_writer = new Fort::RawRunWriter();
    }

    // Vector of run creators
    std::vector<Fort::RunCreator> run_creators;

    // Vector of futures to hold creators' returns
    std::vector<std::future<std::vector<std::string>>> futures;

    for(unsigned int i = 0; i < parallel; ++i )
    {
      run_creators.emplace_back(i, tmp_dir, sorter_mem, locale_name,
                                create_sync, text_reader, pushback,
                                *run_writer);
    }

    // Asynchronously launch run creators
    for( unsigned int i = 0; i < parallel; ++i )
    {
      auto f = std::async(std::launch::async, &Fort::RunCreator::create_runs,
                          &run_creators[i]);

      futures.push_back(std::move(f));
    }

    // Wait for creators to finish and reap their run filenames
    for( auto& f : futures )
    {
      f.wait();
      auto it = f.get();
      run_files.insert(run_files.end(), it.begin(), it.end());
    }

    delete run_writer;
  }

  // ---- Merge runs ----

  {
    // Spin up a run reader for every run file
    std::vector<Fort::RunReader*> run_readers;

    for( auto& run_file : run_files )
    {
      if( compress )
      {
        run_readers.push_back(new Fort::LZ4RunReader(run_file, max_element));
      }
      else
      {
        run_readers.push_back(new Fort::RawRunReader(run_file, max_element));
      }
    }

    // We need a single writer
    Fort::TextWriter text_writer(STDOUT_FILENO);

    // Create the merger
    Fort::RunMerger run_merger(locale_name, run_readers, text_writer);

    // Do the merge
    run_merger.merge();
  }

  // ---- Done ----

  exit(EXIT_SUCCESS);
}

bool parse_args(const int argc, const char* const argv[], size_t& mem_size,
                unsigned int& parallel, unsigned int& max_run_writers,
                unsigned int& max_run_io, std::string& tmp_dir,
                size_t& max_element, std::string& locale_string,
                bool& compress)
{
  // Usage string
  static const std::string usage =
    "\nUsage: fort [option]...\n\n"
    "Sorts stdin to stdout.\n\n"
    "Options:\n\n"
    "  --mem_size size          Total size of main internal buffers\n"
    "                             (default: 95% of free memory)\n"
    "  --parallel num           Number of run-creation jobs to run in\n"
    "                             parallel (default: number of CPUs)\n"
    "  --max-run-writers num    In run-creation phase, limit to num simultaneous\n"
    "                             write jobs (default: 1)\n"
    "  --max-run-io num         In run-creation phase, limit to num simultaneous\n"
    "                             read and/or write jobs (default: 1)\n"
    "  --tmp-dir dir            Store temporary files in directory dir\n"
    "                             (default: /tmp)\n"
    "  --max-element size       Maximum size of any single element in the\n"
    "                             dataset (default: 16M)\n"
    "  --locale locale          Set the collation sequence to use the\n"
    "                             specified locale (default: none)\n"
    "  --no-compress            Do not compress intermediate run files\n\n"
    "size accepts suffixes % (percentage of free main memory at startup),\n"
    "  and SI suffixes (K, M, G, T). With no suffix, bytes are assumed.\n\n"
    "If no locale is specified, the sort will be ordered by byte value.\n\n"
    "For --max-*-io options, an argument of 0 means unlimited.\n\n";

  // Measure free memory and number of CPUs
  size_t free_memory = measure_free_memory();
  unsigned int cpus = sysconf(_SC_NPROCESSORS_ONLN);

  // Set initial/default values
  mem_size = (95 * free_memory) / 100;
  parallel = cpus;
  max_run_writers = 1;
  max_run_io = 1;
  tmp_dir = "/tmp";
  max_element = 1 << 24;
  locale_string = "";
  compress = true;
  
  // Defaults?
  if( argc == 1 )
  {
    return true;
  }

  // Help requested?
  std::string first_key(argv[1]);

  if( first_key == "--help" || first_key == "-h" )
  {
    std::cerr << usage;
    return false;
  }

  // Parse the arguments
  int i = 1;

  try
  {
    while( i < argc )
    {
      std::string key(argv[i]);

      if( key == "--no-compress" )
      {
        compress = false;
        ++i;
      }
      else if( i < (argc - 1) )
      {
        std::istringstream val(argv[i+1]);
  
        if( key == "--mem_size" )
        {
          mem_size = parse_size(val, free_memory);
        }
        else if( key == "--parallel" )
        {
          val >> parallel;
        }
        else if( key == "--max-run-writers" )
        {
          val >> max_run_writers;
        }
        else if( key == "--max-run-io" )
        {
          val >> max_run_io;
        }
        else if( key == "--tmp-dir" )
        {
          val >> tmp_dir;
        }
        else if( key == "--max-element" )
        {
          max_element = parse_size(val, free_memory);
        }
        else if( key == "--locale" )
        {
          val >> locale_string;
        }
        else
        {
          throw std::runtime_error("Unrecognised argument " + key);
        }
  
        i += 2;
      }
    }

    if( i != argc )
    {
      throw std::runtime_error("Unrecognised argument "
                                 + std::string(argv[argc-1]));
    }
  }
  catch( std::exception& e )
  {
    FATAL(e.what());
    return false;
  }

  return true;
}

size_t parse_size(std::istringstream& val, size_t free_memory)
{
  size_t base;
  char suffix = '\0';

  val >> base >> suffix;

  switch( suffix )
  {
    case '%':
      return ((base * free_memory) / 100);
      break;

    case 'b':
    case '\0':
      return base;
      break;

    case 'K':
      return (base << 10);
      break;

    case 'M':
      return (base << 20);
      break;

    case 'G':
      return (base << 30);
      break;

    case 'T':
      return (base << 40);
      break;

    default:
      throw std::runtime_error("Unrecognised size " + val.str());
  }
}

size_t measure_free_memory()
{
  std::ifstream meminfo("/proc/meminfo", std::ifstream::in);
  std::string line;

  size_t kb = 0;
  unsigned int found = 0;

  while( std::getline(meminfo, line) )
  {
    std::istringstream iss(line);
    std::string key;
    size_t val;

    iss >> key >> val;

    // "Free memory" needs to include buffers and cache to be useful
    if( key == "MemFree:" )
    {
      kb = val;
      found++;
    }
    else if( key == "Buffers:" )
    {
      kb += val;
      found++;
    }
    else if( key == "Cached:" )
    {
      kb += val;
      found++;
    }

    if( found == 3 )
    {
      break;
    }
  }

  if( found != 3 )
  {
    throw(std::runtime_error("Failed to correctly parse /proc/meminfo"));
  }

  return (kb << 10);
}
