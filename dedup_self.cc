/*
    Deduplicate items within a group and output indices.

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
#include <array>
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

typedef std::array<uint8_t, BYTE_PER_BUCKET> bucket_t;
typedef std::set<bucket_t> BucketSet;

struct kv {
    std::string _key;
    std::string _value;

    kv(std::string_view key, std::string_view value)
        : _key(key)
    {
        _value = "\"";
        _value += value;
        _value += "\"";
    }
    kv(std::string_view key, size_t value)
        : _key(key)
    {
        _value = std::to_string(value);
    }
    kv(std::string_view key, double value)
        : _key(key)
    {
        _value = std::to_string(value);
    }
};

template <typename CharT, typename Traits>
decltype(auto) operator<<(std::basic_ostream<CharT, Traits>& os, kv rhs)
{
    os << '"' << rhs._key << '"' << ": " << rhs._value;
    return os;
}

int dedup(const std::string& hash_filename, BucketSet* bs)
{
    size_t num_total = 0;
    size_t num_skips = 0;
    size_t num_drops = 0;
    std::ostream& os = std::cout;
    std::ostream& es = std::cerr;

    // Obtain the name for the flag file.
    std::string flag_filename(hash_filename);
    flag_filename += ".f";

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

    // Read the number of hash values per record.
    size_t num_hash_values_per_record;
    ifs.read(reinterpret_cast<char*>(&num_hash_values_per_record), sizeof(num_hash_values_per_record));
    if (num_hash_values_per_record != BUCKET_SIZE * NUM_BUCKETS) {
        es << "ERROR: Hash size is not 4 bytes but " << byte_per_hash << std::endl;
        return 1;
    }
    
    // Open the flag file for reading/writing.
    std::fstream fs(flag_filename, std::ios::in | std::ios::out);
    if (fs.fail()) {
        es << "ERROR: could not open the flag file: " << flag_filename << std::endl;
        return 1;
    }

    // For each record in the flag file.
    for (size_t lineno = 0; lineno < num_records; ++lineno) {
        // Read a flag.
        char flag = fs.get();
        if (fs.eof()) {
            es << "ERROR: premature end of the flag file" << std::endl;
            return 1;
        }

        ++num_total;

        // Do nothing if the record has already been removed.
        bool skip = false;
        if (flag == '0') {
            skip = true;
            ++num_skips;
        } else if (flag != '1') {
            es << "ERROR: a flag must be either '0' or '1': " << flag << std::endl;
            return 1;
        }

        // Read the hash values of the record.
        bucket_t buckets[NUM_BUCKETS];
        for (size_t i = 0;i < NUM_BUCKETS; ++i) {
            ifs.read(reinterpret_cast<char*>(buckets[i].data()), buckets[i].size());
            if (ifs.eof()) {
                es << "ERROR: failed to read the hash value" << std::endl;
                return 1;
            }
        }        
        
        // Check if the hash value is seen before.
	bool drop = false;
        if (!skip) {
            for (size_t i = 0;i < NUM_BUCKETS; ++i) {
                if (bs[i].find(buckets[i]) != bs[i].end()) {
                    // Drop this record.
                    fs.seekp(-1, std::ios_base::cur);
                    fs.put('0');
		    drop = true;
                    ++num_drops;
                    break;
                }
            }
            // Set the buckets.
            if (!drop) {
	        for (size_t i = 0;i < NUM_BUCKETS; ++i) {
                    bs[i].insert(buckets[i]);
                }
	    }
        }
    }

    // Report the stat to STDOUT.
    size_t num_active = num_total - num_skips - num_drops;
    auto pos = hash_filename.find_last_of('/');
    if (pos == std::string::npos) {
        pos = 0;
    } else {
        ++pos;
    }
    std::string target(hash_filename, pos);
    
    os << '{' <<
        kv("target", target) << ", " <<
        kv("num_total", num_total) << ", " <<
        kv("num_active", num_active) << ", " <<
        kv("num_skips", num_skips) << ", " <<
        kv("num_drops", num_drops) << ", " <<
        kv("active_rate", num_active / (double)num_total) << ", " <<
        kv("drop_rate", num_drops / (double)num_total) <<
        '}' << std::endl;

    return 0;
}

int main(int argc, char *argv[])
{
    std::istream& is = std::cin;
    std::ostream& os = std::cout;
    std::ostream& es = std::cerr;
    std::string index_filename(argv[1]);
    
    std::set<bucket_t> bs[NUM_BUCKETS];
    for (;;) {
        // Read a source file.
        std::string line;
        std::getline(is, line);
        if (is.eof()) {
            break;
        }
        // Run deduplication for the file.
        dedup(line, bs);
    }

    // Save the index (sorted buckets) to files.
    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
        // Open the index file.
        std::ostringstream oss;
        oss << index_filename << '.'
            << std::setw(5) << std::setfill('0') << i;
        std::ofstream ofs(oss.str(), std::ios::binary);
        if (ofs.fail()) {
            es << "ERROR: could not open the index file: " << index_filename << std::endl;
            return 1;
        }

        // Write the index file (sorted buckets).
        for (auto it = bs[i].begin(); it != bs[i].end(); ++it) {
            ofs.write(reinterpret_cast<const char*>(it->data()), it->size());
        }
    }

    return 0;
}
