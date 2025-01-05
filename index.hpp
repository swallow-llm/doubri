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

    std::string open(
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
            return std::string("Failed to open the index file: ") + m_filename;
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
        if (m_ofs.fail()) {
            return std::string("Failed to write the header of the index file: ") + m_filename;
        }

        // Exit with an empty error message.
        return std::string("");
    }

    bool update_num_total_items(size_t num_total_items)
    {
        m_num_total_items = num_total_items;
        auto cur = m_ofs.tellp();
        m_ofs.seekp(16);
        write_value<uint64_t>(m_ofs, m_num_total_items);
        m_ofs.seekp(cur);
        return !m_ofs.fail();
    }

    bool update_num_active_items(size_t num_active_items)
    {
        m_num_active_items = num_active_items;
        auto cur = m_ofs.tellp();
        m_ofs.seekp(24);
        write_value<uint64_t>(m_ofs, m_num_active_items);
        m_ofs.seekp(cur);
        return !m_ofs.fail();
    }

    bool write_item(size_t g, size_t i, const uint8_t *bucket)
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
        return !m_ofs.fail();
    }

    bool write_raw(const uint8_t *buffer)
    {
        m_ofs.write(reinterpret_cast<const char*>(buffer), m_bytes_per_bucket + 8);
        return !m_ofs.fail();
    }
};

class IndexReader
{
public:
    std::string m_filename;
    size_t m_bucket_number{0};
    size_t m_bytes_per_bucket{0};
    size_t m_bytes_per_item{0};
    size_t m_num_total_items{0};
    size_t m_num_active_items{0};
    std::ifstream m_ifs;

    std::vector<uint8_t> m_bs;

    IndexReader()
    {        
    }

    virtual ~IndexReader()
    {
    }

    std::string open(const std::string& basename, size_t bucket_number)
    {
        // Obtain the filename for the index.
        std::stringstream ss;
        ss << basename << ".idx." << std::setfill('0') << std::setw(5) << bucket_number;
        m_filename = ss.str();

        // Open the file in binary mode.
        m_ifs.open(m_filename, std::ios::binary);
        if (m_ifs.fail()) {
            return std::string("Failed to open the index file: ") + m_filename;
        }

        // Check the header.
        char magic[9]{};
        m_ifs.read(magic, 8);
        if (std::strcmp(magic, "DoubriI4") != 0) {
            return std::string("Unrecognized header '") + std::string(magic) + std::string("' in the file: ") + m_filename;
        }

        // Read the parameters in the header.
        m_bucket_number = read_value<uint32_t, size_t>(m_ifs);
        m_bytes_per_bucket = read_value<uint32_t, size_t>(m_ifs);
        m_bytes_per_item = m_bytes_per_bucket + 8;
        m_num_total_items = read_value<uint64_t, size_t>(m_ifs);
        m_num_active_items = read_value<uint64_t, size_t>(m_ifs);

        // Allocate the buffer to read an item.
        m_bs.resize(m_bytes_per_item);

        // Exit with an empty error message.
        return std::string("");
    }

    size_t bytes_per_bucket() const
    {
        return m_bytes_per_bucket;
    }

    size_t bytes_per_item() const
    {
        return m_bytes_per_item;
    }

    bool next()
    {
        m_ifs.read(
            reinterpret_cast<std::ifstream::char_type*>(&m_bs.front()),
            m_bytes_per_item
            );
        return !m_ifs.eof();
    }

    const uint8_t *ptr() const
    {
        return &m_bs.front();
    }

    const std::vector<uint8_t>& vec() const
    {
        return m_bs;
    }

    std::string bucket() const
    {
        std::stringstream ss;
        for (size_t i = 0; i < m_bytes_per_bucket; ++i) {
            ss << std::hex << std::setfill('0') << std::setw(2) << (int)m_bs[i];
        }
        return ss.str();
    }

    size_t group() const
    {
        size_t v = 0;
        for (size_t i = 0; i < 2; ++i) {
            v <<= 8;
            v |= m_bs[m_bytes_per_bucket + i];
        }
        return v;
    }

    size_t item() const
    {
        size_t v = 0;
        for (size_t i = 2; i < 8; ++i) {
            v <<= 8;
            v |= m_bs[m_bytes_per_bucket + i];
        }
        return v;
    }
};
