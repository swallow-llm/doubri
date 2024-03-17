/*
    Deduplicate items in other groups by using the indices.

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
#include <cassert>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <BS_thread_pool.hpp>
#include "common.h"

class BucketSet
{
protected:
    size_t m_num;
    bucket_t *m_buffer;
    
public:
    BucketSet() : m_num(0), m_buffer(NULL)
    {
    }

    virtual ~BucketSet()
    {
        release();
    }

    bool load(const std::string& filename)
    {
        // Open the file.
        std::ifstream ifs(filename.c_str(), std::ios::binary);
        if (ifs.fail()) {
            return false;
        }

        // Release the buffer if it has already been allocated.
        release();

        // Find the file size.
        ifs.seekg(0, std::ios::end);
        auto size = ifs.tellg();
        ifs.seekg(0, std::ios::beg);
        assert(size % BYTE_PER_BUCKET == 0);

        // Read the index.
        m_num = size / BYTE_PER_BUCKET;
        m_buffer = new bucket_t[m_num];
        for (size_t i = 0; i < m_num; ++i) {
            bucket_t& bucket = m_buffer[i];
            ifs.read(reinterpret_cast<char*>(bucket.data()), bucket.size());
            if (ifs.fail()) {
                return false;
            }
        }
        
        return true;
    }

    void release()
    {
        if (m_buffer) {
            delete[] m_buffer;
            m_buffer = NULL;
            m_num = 0;
        }
    }

    bool exist(const bucket_t& query) const
    {
        return std::binary_search(m_buffer, m_buffer + m_num, query);
    }
};

void dedup(std::string hash_filename, const BucketSet* bs)
{
    size_t num_total = 0;
    size_t num_skips = 0;
    size_t num_drops = 0;
    BS::synced_stream sos(std::cout);
    BS::synced_stream ses(std::cerr);

    // Obtain the name for the flag file.
    std::string flag_filename(hash_filename);
    flag_filename += ".f";
    
    // Open the hash file for reading (binary).
    std::ifstream ifs(hash_filename, std::ios::binary);
    if (ifs.fail()) {
        std::stringstream ss;
        ss << "ERROR: could not open the hash file: " << hash_filename;
        ses.println(ss.str());
        return;
    }

    // Read the header and check consistencies.
    char magic[8]{};
    ifs.read(magic, 7);
    if (std::strcmp(magic, "MinHash") != 0) {
        std::stringstream ss;
        ss << "ERROR: unrecognized header: " << magic;
        ses.println(ss.str());
        return;
    }

    // Check the consistency of size_t;
    uint8_t byte_per_hash;
    ifs.read(reinterpret_cast<char*>(&byte_per_hash), 1);
    if (byte_per_hash != 4) {
        std::stringstream ss;
        ss << "ERROR: Hash size is not 4 bytes but " << byte_per_hash;
        ses.println(ss.str());
        return;
    }

    // Open the flag file for reading/writing.
    std::fstream fs(flag_filename, std::ios::in | std::ios::out);
    if (fs.fail()) {
        std::stringstream ss;
        ss << "ERROR: could not open the flag file: " << flag_filename;
        ses.println(ss.str());
        return;
    }

    // For each record in the flag file.
    for (size_t lineno = 0; ; ++lineno) {
        // Read a flag.
        char flag = fs.get();
        if (fs.eof()) {
            break;
        }

        ++num_total;

        // Do nothing if the record has already been removed.
        if (flag == '0') {
            ++num_skips;
            continue;
        } else if (flag != '1') {
            std::stringstream ss;
            ss << "ERROR: a flag must be either '0' or '1': " << flag;
            ses.println(ss.str());
            return;
        }

        // Seek to the hash values of the #lineno.
        std::streampos pos = 24 + BYTE_PER_RECORD * lineno;
        ifs.seekg(pos);
        if (ifs.fail()) {
            std::stringstream ss;
            ss << "ERROR: failed to seek to a hash value: " << pos;
            ses.println(ss.str());
            return;
        }

        // Check if any bucket is found in the indices.
        for (size_t i = 0;i < NUM_BUCKETS; ++i) {
            // Read the bucket.
            bucket_t bucket;
            ifs.read(reinterpret_cast<char*>(bucket.data()), bucket.size());
            if (ifs.eof()) {
                std::stringstream ss;
                ss << "ERROR: failed to read the hash value";
                ses.println(ss.str());
                return;
            }
            
            if (bs[i].exist(bucket)) {
                // Drop this record.
                fs.seekp(-1, std::ios_base::cur);
                fs.put('0');
                ++num_drops;
                break;
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

    std::stringstream ss;
    ss << '{' <<
        kv("target", target) << ", " <<
        kv("num_total", num_total) << ", " <<
        kv("num_active", num_active) << ", " <<
        kv("num_skips", num_skips) << ", " <<
        kv("num_drops", num_drops) << ", " <<
        kv("active_rate", num_active / (double)num_total) << ", " <<
        kv("drop_rate", num_drops / (double)num_total) <<
        '}';
    sos.println(ss.str());
}

int main(int argc, char *argv[])
{
    std::istream& is = std::cin;
    std::ostream& os = std::cout;
    std::ostream& es = std::cerr;
    std::string index_filename(argv[1]);
    
    // Open the bucket indices.
    BucketSet bs[NUM_BUCKETS];
    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
        // Open the index file.
        std::ostringstream oss;
        oss << index_filename << '.'
            << std::setw(5) << std::setfill('0') << i;
        bs[i].load(oss.str());
    }

    int total_tasks = 0;
    BS::thread_pool pool(124);
    for (int i = 2; i < argc; ++i) {
        std::ifstream ifs(argv[i]);
        if (ifs.fail()) {
            es << "ERROR: failed to open a target file: " << argv[i] << std::endl;
            return 1;
        }
        
        for (;;) {
            std::string line;
            std::getline(ifs, line);
            if (ifs.eof()) {
                break;
            }
            pool.push_task(dedup, line, bs);
            ++total_tasks;
        }
    }

    BS::synced_stream ses(std::cerr);
    for (;;) {
        std::stringstream ss;
        ss <<
            pool.get_tasks_total() << " tasks total, " <<
            pool.get_tasks_running() << " tasks running, " <<
            pool.get_tasks_queued() << " tasks queued.\r";
        ses.print(ss.str());
        pool.wait_for_tasks_duration(std::chrono::milliseconds(1000));
        if (pool.get_tasks_running() == 0) {
            break;
        }
    }

    return 0;
}
