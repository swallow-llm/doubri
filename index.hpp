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

std::string get_index_filename(const std::string& basename, size_t bucket_number, bool trimmed)
{
    std::stringstream ss;
    if (trimmed) {
        ss << basename << ".idx." << std::setfill('0') << std::setw(5) << bucket_number;
    } else {
        ss << basename << ".index." << std::setfill('0') << std::setw(5) << bucket_number;
    }
    return ss.str();
}

/**
 * A writer for MinHash index (array of buckets).
 */
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
    /**
     * Construct.
     */
    IndexWriter()
    {
    }

    /**
     * Destruct.
     */
    virtual ~IndexWriter()
    {
    }

    /**
     * Open an index file for writing.
     *  @param  basename            The basename for the index file.
     *  @param  bucket_number       The bucket number.
     *  @param  bytes_per_bucket    The bytes per bucket.
     *  @param  num_total_items     The total number of items (including duplicates).
     *  @param  num_active_items    The number of items (excluding duplicates).
     *  @param  trimmed             Whether the items are "trimmed"
     *  @return                     The error message if any (an empty string with no error).
     */
    std::string open(
        const std::string& basename,
        size_t bucket_number = 0,
        size_t bytes_per_bucket = 0,
        size_t num_total_items = 0,
        size_t num_active_items = 0,
        bool trimmed = true
        )
    {
        // Store header parameters.
        m_bucket_number = bucket_number;
        m_bytes_per_bucket = bytes_per_bucket;
        m_num_total_items = num_total_items;
        m_num_active_items = num_active_items;

        // Obtain the filename for the index.
        m_filename = get_index_filename(basename, bucket_number, trimmed);

        // Open the file in binary mode.
        m_ofs.open(m_filename, std::ios::binary);
        if (m_ofs.fail()) {
            return std::string("Failed to open the index file: ") + m_filename;
        }

        // 0x0000: Write the header: "DoubriI4"
        m_ofs.write("DoubriI4", 8);
        // 0x0008: Write the bucket number.
        writeval<uint32_t>(m_ofs, m_bucket_number);
        // 0x000C: Write the number of bytes per bucket.
        writeval<uint32_t>(m_ofs, m_bytes_per_bucket);
        // 0x0010: Write the total number of items (including duplicates).
        writeval<uint64_t>(m_ofs, m_num_total_items);
        // 0x0018: Write the number of active items (excluding duplicates).
        writeval<uint64_t>(m_ofs, m_num_active_items);
        if (m_ofs.fail()) {
            return std::string("Failed to write the header of the index file: ") + m_filename;
        }

        // Exit with an empty error message.
        return std::string("");
    }

    /**
     * Update the total number of items.
     *  @param  num_total_items     The total number of items (including duplicates).
     *  @return                     \c true if successful; \c false otherwise.    
     */
    bool update_num_total_items(size_t num_total_items)
    {
        m_num_total_items = num_total_items;
        auto cur = m_ofs.tellp();
        m_ofs.seekp(16);
        writeval<uint64_t>(m_ofs, m_num_total_items);
        m_ofs.seekp(cur);
        return !m_ofs.fail();
    }

    /**
     * Update the number of active items.
     *  @param  num_active_items    The number of items (excluding duplicates).
     *  @return                     \c true if successful; \c false otherwise.    
     */
    bool update_num_active_items(size_t num_active_items)
    {
        m_num_active_items = num_active_items;
        auto cur = m_ofs.tellp();
        m_ofs.seekp(24);
        writeval<uint64_t>(m_ofs, m_num_active_items);
        m_ofs.seekp(cur);
        return !m_ofs.fail();
    }

    /**
     * Write an element to the bucket array.
     *  This function writes a bucket in \c m_bytes_per_bucket bytes and an
     *  item number in 64 bit (little endian).
     *  @param  i       The item number.
     *  @param  bucket  The pointer to the bucket (byte stream).
     *  @return         \c true if successful; \c false otherwise.    
     */
    bool write_item(size_t i, const uint8_t *bucket)
    {
        uint64_t v = i;
        if constexpr (std::endian::native == std::endian::little) {
            // Use std::byteswap when C++23 is supported by most compilers.
            // v = std::byteswap(v);
            v = bswap_64(v);
        }

        m_ofs.write(reinterpret_cast<const char*>(bucket), m_bytes_per_bucket);
        writeval<uint64_t>(m_ofs, v);
        return !m_ofs.fail();
    }

    /**
     * Write a raw byte stream to the bucket array.
     *  @param  buffer  The pointer to the raw byte stream of the element.
     *  @return         \c true if successful; \c false otherwise.    
     */
    bool write_raw(const uint8_t *buffer)
    {
        m_ofs.write(reinterpret_cast<const char*>(buffer), m_bytes_per_bucket + sizeof(uint64_t));
        return !m_ofs.fail();
    }

protected:
    template <typename StreamT, typename ValueT>
    inline void writeval(std::ostream& os, ValueT value)
    {
        StreamT value_ = static_cast<StreamT>(value);
        os.write(reinterpret_cast<char*>(&value_), sizeof(value_));
    }
};

/**
 * A reader for MinHash index (array of buckets).
 */
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

    /**
     * Construct.
     */
    IndexReader()
    {        
    }

    /**
     * Destruct.
     */
    virtual ~IndexReader()
    {
    }

    /**
     * Open an index file for reading.
     *  @param  basename            The basename for the index file.
     *  @param  bucket_number       The bucket number.
     *  @param  trimmed             Whether the items are "trimmed"
     *  @return                     The error message if any (an empty string with no error).
     */
    std::string open(const std::string& basename, size_t bucket_number, bool trimmed)
    {
        // Obtain the filename for the index.
        m_filename = get_index_filename(basename, bucket_number, trimmed);

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
        m_bucket_number = readval<uint32_t, size_t>(m_ifs);
        m_bytes_per_bucket = readval<uint32_t, size_t>(m_ifs);
        m_bytes_per_item = m_bytes_per_bucket + 8;
        m_num_total_items = readval<uint64_t, size_t>(m_ifs);
        m_num_active_items = readval<uint64_t, size_t>(m_ifs);

        // Allocate the buffer to read an item.
        m_bs.resize(m_bytes_per_item);

        // Exit with an empty error message.
        return std::string("");
    }

    /**
     * Close the index file.
     */
    void close()
    {
        m_ifs.close();
    }

    /**
     * Get the number of bytes per bucket.
     *  @return     The numebr of bytes per bucket.
     */
    size_t bytes_per_bucket() const
    {
        return m_bytes_per_bucket;
    }

    /**
     * Get the number of bytes per item.
     *  This number is \c bytes_per_bucket() + \c sizeof(uint64_t).
     *  @return     The numebr of bytes per item.
     */
    size_t bytes_per_item() const
    {
        return m_bytes_per_item;
    }

    /**
     * Read the next element.
     *  @return     \c true if successful, \c false otherwise.
     */
    bool next()
    {
        m_ifs.read(
            reinterpret_cast<std::ifstream::char_type*>(&m_bs.front()),
            m_bytes_per_item
            );
        return !m_ifs.eof() && !m_ifs.fail();
    }

    /**
     * Get the pointer to the byte stream of the current item.
     *  @return     The pointer to the byte stream of the current item.
     */
    const uint8_t *ptr() const
    {
        return &m_bs.front();
    }

    /**
     * Get the vector storing the byte stream of the current item.
     *  @return     The reference to the vector.
     */
    const std::vector<uint8_t>& vec() const
    {
        return m_bs;
    }

    /**
     * Obtain a string representation of the bucket.
     *  @return     The string representation of the current bucket.
     */
    std::string bucket() const
    {
        std::stringstream ss;
        for (size_t i = 0; i < m_bytes_per_bucket; ++i) {
            ss << std::hex << std::setfill('0') << std::setw(2) << (int)m_bs[i];
        }
        return ss.str();
    }

    /**
     * Obtain an item number of the current element.
     *  @return     The item number.
     */
    size_t inum() const
    {
        size_t v = 0;
        for (size_t i = 0; i < 8; ++i) {
            v <<= 8;
            v |= m_bs[m_bytes_per_bucket + i];
        }
        return v;
    }

protected:
    template <typename StreamT, typename ValueT>
    inline ValueT readval(std::istream& is)
    {
        StreamT value_;
        is.read(reinterpret_cast<char*>(&value_), sizeof(value_));
        return static_cast<ValueT>(value_);
    }
};
