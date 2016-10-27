//
// fort: Raw (uncompressed) run writer
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

#include <fstream>

#include "RawRunWriter.hpp"

namespace Fort
{
  // ---- Public member functions ----

  void RawRunWriter::write(const KeyStore& keystore,
                           const std::string& run_file)
  {
    // Open file
    std::ofstream out(run_file, std::ios::binary);

    for( auto& kv : keystore )
    {
      out.write(reinterpret_cast<const char*>(&kv.second), sizeof(kv.second));
      out.write(kv.first, kv.second);
    }
  }

}
