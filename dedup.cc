/*
    Deduplicate items within a group and output flags and indices.

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

//#define TRIM_INDEX          1
//#define DEBUG_DUPLICATES    1
//#define DEBUG_SORT          1

#include <algorithm>
#include <bit>
#include <concepts>
#include <cstdint>
#include <compare>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <ios>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <argparse/argparse.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/stopwatch.h>
#include <tbb/parallel_sort.h>
#include <tbb/parallel_for_each.h>

#include "common.h"
#include "minhash.hpp"
#include "index.hpp"
#include "flag.hpp"

/**
 * An element (bucket and item number) for deduplication.
 *
 *  This class implements ordering and equality of elements so that we can
 *  deduplicate items by sorting buckets and finding unique ones. For space
 *  efficiency, this program allocates a (huge) array of buckets at a time
 *  rather than allocating a bucket buffer for each Element instance. The
 *  static member \c s_buffer stores the pointer to the bucket array, and
 *  the static member \c s_bytes_per_bucket presents the byte per bucket.
 *  The member \c i presents the item number. Note that the ordering of two
 *  elements is a bit unintuitive:
 *      + operator<(): buckets (ascending) and item numbers (ascending).
 *      + operator>(): buckets (ascending) and item numbers (descending).
 *      + operator==(): equality of buckets.
 *  Deduplicating items with operator<() removes items with greater index
 *  numbers, whereas doing so with operator>() removes items with less index
 *  numbers.
 */
struct Element {
    static uint8_t *s_buffer;
    static size_t s_bytes_per_bucket;
    size_t i;

    /**
     * Less operator for comparing buckets and item numbers.
     *
     * This defines dictionary order of byte streams of buckets. When two
     * buckets are identical, this uses ascending order of item numbers
     * to ensure the stability of element order. This treatment is necessary
     * for the consistency of duplicates recognized across different bucket
     * arrays. Calling std::sort() will sort elements in dictionary order of
     * buckets and ascending order of item numbers.
     *
     *  @param  x   An element.
     *  @param  y   Another element.
     *  @return     \c true if x < y, \c false otherwise.
     */
    friend bool operator<(const Element& x, const Element& y)
    {
        auto order = std::memcmp(x.ptr(), y.ptr(), s_bytes_per_bucket);
        return order == 0 ? (x.i < y.i) : (order < 0);
    }

    /**
     * Greater operator for comparing buckets and item numbers.
     *
     * This defines dictionary order of byte streams of buckets. When two
     * buckets are identical, this uses descending order of item numbers
     * to ensure the stability of element order. This treatment is necessary
     * for the consistency of duplicates recognized across different bucket
     * arrays. Calling std::sort() will sort elements in dictionary order of
     * buckets and descending order of item numbers.
     *
     *  @param  x   An element.
     *  @param  y   Another element.
     *  @return     \c true if x > y, \c false otherwise.
     */
    friend bool operator>(const Element& x, const Element& y)
    {
        auto order = std::memcmp(x.ptr(), y.ptr(), s_bytes_per_bucket);
        return order == 0 ? (x.i > y.i) : (order > 0);
    }

    /**
     * Equality operator for comparing buckets.
     *
     * This defines the equality of two buckets. Unlike the operator< and
     * operator>, this function does not consider item numbers for equality.
     * 
     *  @param  x   An item.
     *  @param  y   Another item.
     *  @return bool    \c true when two elements have the same bucket,
     *                  \c false otherwise.
     */
    friend bool operator==(const Element& x, const Element& y)
    {
        return std::memcmp(x.ptr(), y.ptr(), s_bytes_per_bucket) == 0;
    }

    /**
     * Get a pointer to the bucket of the item.
     *
     *  @return uint8_t*    The pointer to the bucket in the bucket array.
     */
    uint8_t *ptr()
    {
        return s_buffer + i * s_bytes_per_bucket;
    }

    /**
     * Get a pointer to the bucket of the item.
     *
     *  @return const uint8_t*  The pointer to the bucket in the bucket array.
     */
    const uint8_t *ptr() const
    {
        return s_buffer + i * s_bytes_per_bucket;
    }

    /**
     * Get the split of the bucket.
     *  Currently, we split buckets based on the last byte.
     *
     *  @return The split.
     */
    const uint8_t split() const
    {
        const uint8_t *p = this->ptr();
        return p[s_bytes_per_bucket-1];
    }

    /**
     * Represents an element with the item number and bucket.
     *
     *  @return std::string The string representing the item.
     */
    std::string repr() const
    {
        std::stringstream ss;
        ss << std::setfill('0') << std::setw(15) << i << ": ";
        for (size_t j = 0; j < s_bytes_per_bucket; ++j) {
            ss << std::hex << std::setfill('0') << std::setw(2) << (int)*(ptr() + j);
        }
        return ss.str();
    }
};

uint8_t *Element::s_buffer = nullptr;
size_t Element::s_bytes_per_bucket = 0;

struct HashFile {
    std::string filename;
    size_t num_items{0};
    size_t start_number{0};

    HashFile(const std::string& filename = "") : filename(filename)
    {
    }
};

class MinHashLSHException : public std::runtime_error
{
public:
    MinHashLSHException(const std::string& message = "") : std::runtime_error(message)
    {
    }

    virtual ~MinHashLSHException()
    {
    }
};

class MinHashLSH {
public:
    std::vector<HashFile> m_hfs;
    size_t m_num_items{0};
    size_t m_bytes_per_hash{0};
    size_t m_num_hash_values{0};
    size_t m_begin{0};
    size_t m_end{0};

protected:
    uint8_t* m_buffer{nullptr};
    std::vector<Element> m_items;
    std::vector<char> m_flags;
    spdlog::logger& m_logger;
    uint16_t m_group;

public:
    MinHashLSH(spdlog::logger& logger, uint16_t group) :
        m_logger(logger), m_group(group)
    {
    }

    virtual ~MinHashLSH()
    {
        clear();
    }

    void clear()
    {
        if (m_buffer) {
            delete[] m_buffer;
            m_buffer = nullptr;
        }
        m_items.clear();
        m_flags.clear();
    }

    void append_file(const std::string& filename)
    {
        m_hfs.push_back(HashFile(filename));
    }

    void initialize()
    {
        // Initialize the parameters.
        m_num_items = 0;
        m_bytes_per_hash = 0;
        m_num_hash_values = 0;
        m_begin = 0;
        m_end = 0;

        m_logger.info("group: {}", m_group);
	
        // Open the hash files to retrieve parameters.
        m_logger.info("num_minhash_files: {}", m_hfs.size());
        for (auto& hf : m_hfs) {
            hf.start_number = m_num_items;

            // Open the hash file.
            m_logger.trace("Open a hash file: {}", hf.filename);
            MinHashReader mr;
            mr.open(hf.filename);

            // Store the parameters of the first file or check the cnosistency of other files.
            if (m_bytes_per_hash == 0) {
                // This is the first file.
                m_num_items = mr.m_num_items;
                m_bytes_per_hash = mr.m_bytes_per_hash;
                m_num_hash_values = mr.m_num_hash_values;
                m_begin = mr.m_begin;
                m_end = mr.m_end;
                m_logger.info("bytes_per_hash: {}", m_bytes_per_hash);
                m_logger.info("num_hash_values: {}", m_num_hash_values);
                m_logger.info("begin: {}", m_begin);
                m_logger.info("end: {}", m_end);
            } else {
                // Check the consistency of the parameters.
                if (m_bytes_per_hash != mr.m_bytes_per_hash) {
                    m_logger.critical("Inconsistent parameter, bytes_per_hash: {}", mr.m_bytes_per_hash);
                    throw MinHashLSHException();
                }
                if (m_num_hash_values != mr.m_num_hash_values) {
                    m_logger.critical("Inconsistent parameter, num_hash_values: {}", mr.m_num_hash_values);
                    throw MinHashLSHException();
                }
                if (m_begin != mr.m_begin) {
                    m_logger.critical("Inconsistent parameter, begin: {}", mr.m_begin);
                    throw MinHashLSHException();
                }
                if (m_end != mr.m_end) {
                    m_logger.critical("Inconsistent parameter, end: {}", mr.m_end);
                    throw MinHashLSHException();
                }
                m_num_items += mr.m_num_items;
            }
            hf.num_items = mr.m_num_items;
        }

        m_logger.info("num_total_items: {}", m_num_items);

        // Clear existing arrays.
        clear();

        // Allocate an array for buckets (this can be huge).
        size_t size = m_bytes_per_hash * m_num_hash_values * m_num_items;
        m_logger.info("Allocate an array for buckets ({:.3f} MB)", size / (double)1e6);
        m_buffer = new uint8_t[size];

        // Allocate an index for buckets.
        m_logger.info("Allocate an array for items ({:.3f} MB)", m_num_items * sizeof(Element) / (double)1e6);
        m_items.resize(m_num_items);

        // Allocate an array for non-duplicate flags.
        m_logger.info("Allocate an array for flags ({:.3f} MB)", m_num_items * sizeof(char) / (double)1e6);
        m_flags.resize(m_num_items, ' ');

        // Set static members of Element to access the bucket array.
        Element::s_buffer = m_buffer;
        Element::s_bytes_per_bucket = m_bytes_per_hash * m_num_hash_values;
    }

    void save_flag(const std::string& filename)
    {
        m_logger.info("Save flags to a file: {}", filename);

        std::string msg = flag_save(filename, m_flags);
        if (!msg.empty()) {
            m_logger.critical(msg);
            throw MinHashLSHException();
        }
    }

    void deduplicate_bucket(const std::string& basename, size_t bucket_number, bool reverse = false, bool save_index = true)
    {
        spdlog::stopwatch sw;
        
        const size_t bytes_per_bucket = m_bytes_per_hash * m_num_hash_values;

        // Read MinHash buckets from files.
        spdlog::stopwatch sw_read;
        m_logger.info("[#{}] Read buckets from {} files", bucket_number, m_hfs.size());

        tbb::parallel_for_each(m_hfs.begin(), m_hfs.end(), [&](HashFile& hf) {
            m_logger.trace("[#{}] Read {} buckets from {}", bucket_number, hf.num_items, hf.filename);

            // Open the hash file.
            MinHashReader mr;
            mr.open(hf.filename);
            mr.read_bucket_array(m_buffer + hf.start_number * bytes_per_bucket, bucket_number);
        });
        m_logger.info("[#{}] Completed reading in {:.3f} seconds", bucket_number, sw_read);

        // Set item numbers to elements.
        for (size_t i = 0; i < m_num_items; ++i) {
            m_items[i].i = i;
        }

        // Sort the buckets of items.
        spdlog::stopwatch sw_sort;
        m_logger.info("[#{}] Sort buckets", bucket_number);
        if (reverse) {
            tbb::parallel_sort(m_items.begin(), m_items.end(), std::greater<Element>());
        } else {
            tbb::parallel_sort(m_items.begin(), m_items.end(), std::less<Element>());
        }
        m_logger.info("[#{}] Completed sorting in {:.3f} seconds", bucket_number, sw_sort);

#ifdef  DEBUG_SORT
        for (auto it = m_items.begin(); it != m_items.end(); ++it) {
            std::cout << it->repr() << std::endl;
        }
#endif/*DEBUG_SORT*/

        // Count the number of inactive items.
        size_t num_active_before = std::count(m_flags.begin(), m_flags.end(), ' ');

        // Save the index files.
        spdlog::stopwatch sw_index;
    	m_logger.info("[#{}] Write non-duplicate items to the untrimmed index: {}", bucket_number, basename);

        // Split the index into 256 files. Because reading and writing
        // a large file is slow, we split the index into 256 files so that
        // we can read, merge, and write individual index files in parallel.
        tbb::parallel_for((size_t)SPLIT_BEGIN, (size_t)SPLIT_END, [&](size_t split) {
        //for (size_t split = SPLIT_BEGIN; split < SPLIT_END; ++split) {

            // Open an index file (untrimmed).
            IndexWriter writer;            
            std::string msg = writer.open(
                basename,
                bucket_number,
                (uint8_t)split,
                bytes_per_bucket,
                0, // We will find and update this number later.
                0, // We will find and update this number later.
#ifdef  TRIM_INDEX
                false
#else
                true
#endif/*TRIM_INDEX*/
                );
            if (!msg.empty()) {
                m_logger.critical(msg);
                throw MinHashLSHException();
            }

            size_t num_total_items = 0;
	        size_t num_active_items = 0;
            for (auto cur = m_items.begin(); cur != m_items.end(); ) {
                // Skip items that do not belong to the split.
                if (cur->split() != split) {
                    ++cur;
                    continue;
                }

                // Find the next item that has a different bucket from the current one.
                auto next = cur + 1;
                while (next != m_items.end() && *cur == *next) {
	                ++next;
                }

#ifdef  DEBUG_DUPLICATES
                std::string duplicates;
                bool has_duplicates = (next != cur + 1);
                if (has_duplicates) {
//                    duplicates += std::to_string(m_group);
                    duplicates += "0:";
                    duplicates += std::to_string(8429 * m_group + cur->i);
                }
#endif/*DEBUG_DUPLICATES*/

                // Write the current item to the index if it is not a duplicate.
                if (m_flags[cur->i] == ' ') {
	                writer.write_item(m_group, cur->i, cur->ptr());
	                ++num_active_items;
	            }
                ++num_total_items;

                // Mark a duplication flag for every item of the same bucket.
                for (++cur; cur != next; ++cur) {
	                // Mark the item with a local duplicate flag (within this trial).
	                m_flags[cur->i] = 'd';
                    ++num_total_items;
#ifdef  DEBUG_DUPLICATES
                    duplicates += ' ';
//                    duplicates += std::to_string(m_group);
                    duplicates += "0:";
                    duplicates += std::to_string(8429 * m_group + cur->i);
#endif/*DEBUG_DUPLICATES*/
                }

#ifdef  DEBUG_DUPLICATES
                if (!duplicates.empty()) {
                    m_logger.info("[#{}] Duplicate(s): {}", bucket_number, duplicates);
                }
#endif/*DEBUG_DUPLICATES*/
	        }
            m_logger.info("[#{}] Split {:02x} has {} / {} items.", bucket_number, split, num_active_items, num_total_items);

            // Update the header of the index with item numbers.
	        writer.update_num_active_items(num_active_items);
            writer.update_num_total_items(num_total_items);
	    });
    	m_logger.info("[#{}] Completed writing the index in {:.3f} seconds", bucket_number, sw_index);
	
        // Find the numbers of active and detected items.
        size_t num_active_after = std::count(m_flags.begin(), m_flags.end(), ' ');
        size_t num_detected = std::count(m_flags.begin(), m_flags.end(), 'd');

        // Change local duplicate flags into global ones.
        for (auto it = m_flags.begin(); it != m_flags.end(); ++it) {
            *it = std::toupper(*it);    // 'd' -> 'D'.
        }

        // Report statistics.
        double active_ratio = 0 < m_num_items ? num_active_after / (double)m_num_items : 0.;
        double detection_ratio = 0 < m_num_items ? num_detected / (double)m_num_items : 0.;
        m_logger.info(
            "[#{}] Deduplication completed: {{"
            "\"num_active_before\": {}, "
            "\"num_detected\": {}, "
            "\"num_active_after\": {}, "
            "\"active_ratio\": {:.05f}, "
            "\"detection_ratio\": {:.05f}, "
            "\"time\": {:.03f}"
            "}}",
            bucket_number,
            num_active_before,
            num_detected,
            num_active_after,
            active_ratio,
            detection_ratio,
            sw
            );
    }

    void run(const std::string& basename, bool reverse = false)
    {
        spdlog::stopwatch sw;
        size_t num_active_before = std::count(m_flags.begin(), m_flags.end(), ' ');

        // Deduplication for each bucket number.
        for (size_t bn = m_begin; bn < m_end; ++bn) {
            m_logger.info("Deduplication for #{}", bn);
            deduplicate_bucket(basename, bn, reverse);
        }

#ifdef  TRIM_INDEX
        // Trimming inactive items across different bucket numbers.
        for (size_t bn = m_begin; bn < m_end; ++bn) {
            //trim_bucket_array(basename, bn);
        }
#endif/*TRIM_INDEX*/

        size_t num_active_after = std::count(m_flags.begin(), m_flags.end(), ' ');
        double active_ratio_before = 0 < m_num_items ? num_active_before / (double)m_num_items : 0.;
        double active_ratio_after = 0 < m_num_items ? num_active_after / (double)m_num_items : 0.;
        // Report overall stFatistics.
        m_logger.info(
            "Result: {{"
            "\"num_items\": {}, "
            "\"bytes_per_hash\": {}, "
            "\"num_hash_values\": {}, "
            "\"begin\": {}, "
            "\"end\": {}, "
            "\"num_active_before\": {}, "
            "\"num_active_after\": {}, "
            "\"active_ratio_before\": {:.05f}, "
            "\"active_ratio_after\": {:.05f}, "
            "\"time\": {:.03f}"
            "}}",
            m_num_items,
            m_bytes_per_hash,
            m_num_hash_values,
            m_begin,
            m_end,
            num_active_before,
            num_active_after,
            active_ratio_before,
            active_ratio_after,
            sw
            );
    }

    size_t get_active_items_in_index(const std::string& basename, size_t bucket_number, bool trimmed)
    {
        size_t num_active_items = 0;
        for (size_t split = SPLIT_BEGIN; split < SPLIT_END; ++split) {
            IndexReader reader;
            std::string msg = reader.open(basename, bucket_number, (uint8_t)split, trimmed);
            num_active_items += reader.m_num_active_items;
        }
        return num_active_items;
    }

    void trim_bucket_array(const std::string& basename, size_t bucket_number)
    {
        spdlog::stopwatch sw;

        // Find the number of active items.
        size_t num_active_before = get_active_items_in_index(basename, bucket_number, false);
        m_logger.info("[#{}] Trim inactive ones in {} items.", bucket_number, num_active_before);

        tbb::parallel_for((size_t)SPLIT_BEGIN, (size_t)SPLIT_END, [&](size_t split) {
            spdlog::stopwatch sw_split;

            std::string msg;
            // Open an untrimmed index file for reading.
            IndexReader reader;
            msg = reader.open(basename, bucket_number, (uint8_t)split, false);
            if (!msg.empty()) {
                m_logger.critical(msg);
                throw MinHashLSHException();
            }

            // Open a trimmed index file for writing.
            IndexWriter writer;
            msg = writer.open(
                basename,
                bucket_number,
                (uint8_t)split,
                reader.m_bytes_per_bucket,
                reader.m_num_total_items,
                0, // We will find and update this number later.
                true);
            if (!msg.empty()) {
                m_logger.critical(msg);
                throw MinHashLSHException();
            }

            // Write items that are active across different bucket arrays.
            size_t num_active_items = 0;
            for (size_t j = 0; j < reader.m_num_active_items; ++j) {
                if (!reader.next()) {
                    m_logger.critical("Premature EOF of the index file: {}", reader.m_filename);
                    throw MinHashLSHException();
                }
                // Write the item if the current item is active.
                if (m_flags[reader.inum()] == ' ') {
                    ++num_active_items;
                    if (!writer.write_raw(reader.ptr())) {
                        m_logger.critical("Failed to write an element to the index file: {}", writer.m_filename);
                        throw MinHashLSHException();
                    }
                }
            }

            // Write the number of active items.
            writer.update_num_active_items(num_active_items);

            // Delete the untrimmed index file.
            const std::string file_to_be_deleted = reader.m_filename;
            reader.close();
            std::filesystem::remove(file_to_be_deleted);

            m_logger.info("[#{}:{:02x}] Completed trimming the index in {:.3f} seconds: {} -> {}", bucket_number, split, sw_split, reader.m_num_active_items, num_active_items);
        });

        // Find the number of active items after trimming.
        size_t num_active_after = get_active_items_in_index(basename, bucket_number, true);
        m_logger.info(
            "[#{}] Trimming completed: {{"
            "\"num_active_before\": {}, "
            "\"num_active_after\": {}, "
            "\"time\": {:.03f}"
            "}}",
            bucket_number,
            num_active_before,
            num_active_after,
            sw
            );
    }
};

int main(int argc, char *argv[])
{
    // Build a command-line parser.
    argparse::ArgumentParser program("doubri-dedup", __DOUBRI_VERSION__);
    program.add_description("Read MinHash buckets from files, deduplicate items, and build bucket indices.");
    program.add_argument("-r", "--reverse")
        .help("keep older duplicated items (newer items, by default)")
        .default_value(false)
        .flag();
    program.add_argument("-g", "--group")
        .help("specifies a group number (in [0, 65535]) of the items")
        .scan<'d', uint16_t>()
        .nargs(1)
        .required();
    program.add_argument("-l", "--log-level-console")
        .help("sets a log level for console")
        .default_value(std::string{"warning"})
        .choices("off", "trace", "debug", "info", "warning", "error", "critical")
        .nargs(1);
    program.add_argument("-L", "--log-level-file")
        .help("sets a log level for file logging ({BASENAME}.log)")
        .default_value(std::string{"info"})
        .choices("off", "trace", "debug", "info", "warning", "error", "critical")
        .nargs(1);
    program.add_argument("basename").metavar("BASENAME")
        .help("basename for output files (index, flag, source list, log)")
        .nargs(1)
        .required();

    // Parse the command-line arguments.
    try {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        std::cerr << program;
        return 1;
    }

    // Retrieve parameters.
    const auto basename = program.get<std::string>("basename");
    const auto reverse = program.get<bool>("reverse");
    const auto group = program.get<uint16_t>("group");
    const std::string flagfile = basename + std::string(".dup");
    const std::string logfile = basename + std::string(".log");
    const std::string srcfile = basename + std::string(".src");

    // Initialize the console logger.
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto console_log_level = spdlog::level::from_str(program.get<std::string>("log-level-console"));
    console_sink->set_level(console_log_level);

    // Initialize the file logger.
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(logfile, true);
    auto file_log_level = spdlog::level::from_str(program.get<std::string>("log-level-file"));
    file_sink->set_level(file_log_level);

    // Create the logger that integrates console and file loggers.
    spdlog::logger logger("doubri-dedup", {console_sink, file_sink});
    logger.flush_on(file_log_level);

    // The deduplication object.
    MinHashLSH dedup(logger, group);

    // One MinHash file per line.
    for (;;) {
        // Read a MinHash file (line) from STDIN.
        std::string line;
        std::getline(std::cin, line);
        if (std::cin.eof()) {
            break;
        }
        // Register the MinHash file.
        dedup.append_file(line);
    }

    // Read the MinHash files to initialize the deduplication engine.
    dedup.initialize();

    // Write a list of source files.
    {
        // Open the source-list file.
        std::ofstream ofs(srcfile);
        if (ofs.fail()) {
            logger.critical("Failed to open the source-list file: {}", srcfile);
            return 1;
        }

        // Write the source files and their numbers of items.
        for (auto& hf : dedup.m_hfs) {
            ofs << hf.num_items << '\t' << hf.filename << std::endl;
        }

        if (ofs.fail()) {
            logger.critical("Failed to write the list of source files");
            return 1;
        }
    }

    // Perform deduplication.
    dedup.run(basename, reverse);

    // Store the flag file.
    dedup.save_flag(flagfile);

    return 0;
}
