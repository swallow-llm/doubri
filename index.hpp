/*
    Reader and writer for index files (.idx.#####).

Copyright (c) 2023-2025, Naoaki Okazaki

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

#pragma once

#include <bit>
#include <fstream>
#include <sstream>
#include <string>
#include <byteswap.h>

class IndexWriter
{
public:
    std::string m_filename;
    size_t m_bucket_number{0};
    size_t m_bytes_per_bucket{0};
    size_t m_num_total_items{0};
    size_t m_num_active_items{0};
    std::ofstream m_ofs;

public:
    IndexWriter()
    {
    }

    virtual ~IndexWriter()
    {
    }

    bool open(
        const std::string& basename,
        size_t bucket_number = 0,
        size_t bytes_per_bucket = 0,
        size_t num_total_items = 0,
        size_t num_active_items = 0
        )
    {
        // Store header parameters.
        m_bucket_number = bucket_number;
        m_bytes_per_bucket = bytes_per_bucket;
        m_num_total_items = num_total_items;
        m_num_active_items = num_active_items;

        // Obtain the filename for the index.
        std::stringstream ss;
        ss << basename << ".idx." << std::setfill('0') << std::setw(5) << bucket_number;
        m_filename = ss.str();

        // Open the file in binary mode.
        m_ofs.open(m_filename, std::ios::binary);
        if (m_ofs.fail()) {
            return false;
        }

        // Write the header: "DoubriI4"
        m_ofs.write("DoubriI4", 8);
        // Write the number of hash values per bucket.
        write_value<uint32_t>(m_ofs, m_bucket_number);
        // Write the number of bytes per hash.
        write_value<uint32_t>(m_ofs, m_bytes_per_bucket);
        // Write the total number of items (including duplicates).
        write_value<uint64_t>(m_ofs, m_num_total_items);
        // Write the number of active items (excluding duplicates).
        write_value<uint64_t>(m_ofs, m_num_active_items);

        return !fail();
    }

    bool fail()
    {
        return m_ofs.fail();
    }

    void update_num_total_items(size_t num_total_items)
    {
        m_num_total_items = num_total_items;
        auto cur = m_ofs.tellp();
        m_ofs.seekp(16);
        write_value<uint64_t>(m_ofs, m_num_total_items);
        m_ofs.seekp(cur);
    }

    void update_num_active_items(size_t num_active_items)
    {
        m_num_active_items = num_active_items;
        auto cur = m_ofs.tellp();
        m_ofs.seekp(24);
        write_value<uint64_t>(m_ofs, m_num_active_items);
        m_ofs.seekp(cur);
    }

    void write_item(size_t g, size_t i, const uint8_t *bucket)
    {
        // Make sure that the group number is within 16 bits.
        if (0xFFFF < g) {
            std::stringstream ss;
            ss << "Group number is out of range: " << g;
            throw std::range_error(ss.str());
        }
        // Make sure that the index number is within 48 bits.
        if (0x0000FFFFFFFFFFFF < i) {
            std::stringstream ss;
            ss << "Index number is out of range: " << i;
            throw std::range_error(ss.str());            
        }

        // Build a 64 bit value.
        uint64_t v = ((uint64_t)g << 48) | i;
        if constexpr (std::endian::native == std::endian::little) {
            // Use std::byteswap when C++23 is supported by most compilers.
            // v = std::byteswap(v);
            v = bswap_64(v);
        }

        m_ofs.write(reinterpret_cast<const char*>(bucket), m_bytes_per_bucket);
        write_value<uint64_t>(m_ofs, v);
    }
};

class IndexReader
{
public:
    std::string m_filename;
    size_t m_bucket_number{0};
    size_t m_bytes_per_bucket{0};
    size_t m_num_total_items{0};
    size_t m_num_active_items{0};
    std::ifstream m_ifs;

    IndexReader()
    {        
    }

    virtual ~IndexReader()
    {
    }

    bool open(const std::string& basename, size_t bucket_number)
    {
        // Obtain the filename for the index.
        std::stringstream ss;
        ss << basename << ".idx." << std::setfill('0') << std::setw(5) << bucket_number;
        m_filename = ss.str();

        // Open the file in binary mode.
        m_ifs.open(m_filename, std::ios::binary);
        if (m_ifs.fail()) {
            return false;
        }

        // Check the header.
        char magic[9]{};
        m_ifs.read(magic, 8);
        if (std::strcmp(magic, "DoubriI4") != 0) {
            return false;
        }

        // Read the parameters in the header.
        m_bucket_number = read_value<uint32_t, size_t>(m_ifs);
        m_bytes_per_bucket = read_value<uint32_t, size_t>(m_ifs);
        m_num_total_items = read_value<uint64_t, size_t>(m_ifs);
        m_num_active_items = read_value<uint64_t, size_t>(m_ifs);

        return !fail();
    }

    bool fail()
    {
        return m_ifs.fail();
    }
};

