/*
    Initialize active flags.

Copyright (c) 2023-2024, Naoaki Okazaki

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <string_view>

#define BYTE_PER_HASH 4
#define BUCKET_SIZE 20
#define BYTE_PER_BUCKET (BYTE_PER_HASH * BUCKET_SIZE)
#define NUM_BUCKETS 40
#define BYTE_PER_RECORD (BYTE_PER_BUCKET * NUM_BUCKETS)

int main(int argc, char *argv[])
{
    std::ostream& os = std::cout;
    std::ostream& es = std::cerr;
    const char *hash_filename = argv[1];

    // Open the hash file for reading (binary).
    std::ifstream ifs(hash_filename, std::ios::binary);
    if (ifs.fail()) {
        es << "ERROR: could not open the hash file: " << hash_filename << std::endl;
        return 1;
    }

    // Read the header and check consistencies.
    char magic[8]{};
    ifs.read(magic, 7);
    if (std::strcmp(magic, "MinHash") != 0) {
        es << "ERROR: unrecognized header: " << magic << std::endl;
        return 1;
    }

    // Check the consistency of size_t;
    uint8_t byte_per_hash;
    ifs.read(reinterpret_cast<char*>(&byte_per_hash), 1);
    if (byte_per_hash != 4) {
        es << "ERROR: Hash size is not 4 bytes but " << byte_per_hash << std::endl;
        return 1;
    }

    // Read the number of records.
    size_t num_records;
    ifs.read(reinterpret_cast<char*>(&num_records), sizeof(num_records));

    // Output '1' for all records.
    for (size_t i = 0; i < num_records; ++i) {
        os << '1';
    }

    return 0;
}
