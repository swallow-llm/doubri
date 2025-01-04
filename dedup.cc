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

#include <algorithm>
#include <bit>
#include <concepts>
#include <cstdint>
#include <compare>
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
#include "index.hpp"
#include "flag.hpp"

/**
 * An item for deduplication.
 *
 *  For space efficiency, this program allocates a (huge) array of buckets
 *  at a time rather than allocating a bucket buffer for each Item instance.
 *  The static member \c s_buffer stores the pointer to the bucket array,
 *  and the static member \c s_bytes_per_bucket presents the byte per bucket.
 *  The member \c i presents the index number of the item.
 */
struct Item {
    static uint8_t *s_buffer;
    static size_t s_bytes_per_bucket;
    size_t i;

    /**
     * Spaceship operator for comparing buckets and item indices.
     *
     * This defines dictionary order of byte streams of buckets. When two
     * buckets are identical, this uses ascending order of index numbers
     * to ensure the stability of item order. This treatment is necessary
     * for the consistency of duplicates recognized across different bucket
     * arrays. Calling std::sort() will sort items in dictionary order of
     * buckets and ascending order of index numbers.
     *
     *  @param  x   An item.
     *  @param  y   Another item.
     *  @return std::strong_ordering    The order of the two items.
     */
    friend auto operator<=>(const Item& x, const Item& y) -> std::strong_ordering
    {
        auto order = std::memcmp(x.ptr(), y.ptr(), s_bytes_per_bucket);
        return order == 0 ? (x.i <=> y.i) : (order <=> 0);
    }

    /**
     * Equality operator for comparing buckets.
     *
     * This defines the equality of two buckets. Unlike the operator <=>,
     * this function does not consider item index numbers for equality.

     *  @param  x   An item.
     *  @param  y   Another item.
     *  @return bool    \c true when two items have the same bucket,
     *                  \c false otherwise.
     */
    friend bool operator==(const Item& x, const Item& y)
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
     * Represents an item with the index number and bucket.
     *
     *  @return std::string The string representing the item.
     */
    std::string repr() const
    {
        std::stringstream ss;
        ss << std::setfill('0') << std::setw(15) << i;
        for (size_t j = 0; j < s_bytes_per_bucket; ++j) {
            ss << std::setfill('0') << std::setw(2) << std::ios::hex << *(ptr() + j);
        }
        return ss.str();
    }
};

uint8_t *Item::s_buffer = nullptr;
size_t Item::s_bytes_per_bucket = 0;

struct HashFile {
    std::string filename;
    size_t num_items{0};
    size_t start_index{0};

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
    std::vector<Item> m_items;
    std::vector<char> m_flags;
    spdlog::logger& m_logger;

public:
    MinHashLSH(spdlog::logger& logger) : m_logger(logger)
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

        // Open the hash files to retrieve parameters.
        m_logger.info("# hash files: {}", m_hfs.size());
        for (auto& hf : m_hfs) {
            hf.start_index = m_num_items;

            // Open the hash file.
            m_logger.trace("Open a hash file: {}", hf.filename);
            std::ifstream ifs(hf.filename, std::ios::binary);
            if (ifs.fail()) {
                m_logger.critical("Failed to open a hash file: {}", hf.filename);
                throw MinHashLSHException();
            }

            // Check the header.
            char magic[9]{};
            ifs.read(magic, 8);
            if (std::strcmp(magic, "DoubriH4") != 0) {
                m_logger.critical("Unrecognized header '{}'", magic);
                throw MinHashLSHException();
            }

            // Read the parameters in the hash file.
            size_t num_items = read_value<uint32_t, size_t>(ifs);
            size_t bytes_per_hash = read_value<uint32_t, size_t>(ifs);
            size_t num_hash_values = read_value<uint32_t, size_t>(ifs);
            size_t begin = read_value<uint32_t, size_t>(ifs);
            size_t end = read_value<uint32_t, size_t>(ifs);

            // Store the parameters of the first file or check the cnosistency of other files.
            if (m_bytes_per_hash == 0) {
                // This is the first file.
                m_num_items = num_items;
                m_bytes_per_hash = bytes_per_hash;
                m_num_hash_values = num_hash_values;
                m_begin = begin;
                m_end = end;
                m_logger.info("bytes_per_hash: {}", m_bytes_per_hash);
                m_logger.info("num_hash_values: {}", m_num_hash_values);
                m_logger.info("begin: {}", m_begin);
                m_logger.info("end: {}", m_end);
            } else {
                // Check the consistency of the parameters.
                if (m_bytes_per_hash != bytes_per_hash) {
                    m_logger.critical("Inconsistent parameter, bytes_per_hash: {}", bytes_per_hash);
                    throw MinHashLSHException();
                }
                if (m_num_hash_values != num_hash_values) {
                    m_logger.critical("Inconsistent parameter, num_hash_values: {}", num_hash_values);
                    throw MinHashLSHException();
                }
                if (m_begin != begin) {
                    m_logger.critical("Inconsistent parameter, begin: {}", begin);
                    throw MinHashLSHException();
                }
                if (m_end != end) {
                    m_logger.critical("Inconsistent parameter, end: {}", end);
                    throw MinHashLSHException();
                }
                m_num_items += num_items;
            }
            hf.num_items = num_items;
        }

        m_logger.info("# items: {}", m_num_items);

        // Clear existing arrays.
        clear();

        // Allocate an array for buckets (this can be huge).
        size_t size = m_bytes_per_hash * m_num_hash_values * m_num_items;
        m_logger.info("Allocate an array for buckets ({:.3f} MB)", size / (double)1e6);
        m_buffer = new uint8_t[size];

        // Allocate an index for buckets.
        m_logger.info("Allocate an array for items ({:.3f} MB)", m_num_items * sizeof(Item) / (double)1e6);
        m_items.resize(m_num_items);

        // Allocate an array for non-duplicate flags.
        m_logger.info("Allocate an array for flags ({:.3f} MB)", m_num_items * sizeof(char) / (double)1e6);
        m_flags.resize(m_num_items, ' ');

        // Set static members of Item to access the bucket array.
        Item::s_buffer = m_buffer;
        Item::s_bytes_per_bucket = m_bytes_per_hash * m_num_hash_values;
    }

    void load_flag(const std::string& filename)
    {
        m_logger.info("Load flags from a file: {}", filename);

        std::string msg = flag_load(filename, m_flags);
        if (!msg.empty()) {
            m_logger.critical(msg);
            throw MinHashLSHException();
        }

        // Check the size of the flag file.
        if (m_flags.size() != m_num_items) {
            m_logger.critical("Flag file {} has {} items although the total number of items is {}", filename, m_flags.size(), m_num_items);
            throw MinHashLSHException();
        }
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

    void deduplicate_bucket(const std::string& basename, size_t group, size_t bucket_number, bool save_index = true)
    {
        spdlog::stopwatch sw;
        
        const size_t bytes_per_bucket = m_bytes_per_hash * m_num_hash_values;
        const size_t bytes_per_item = bytes_per_bucket * (m_end - m_begin);
        const size_t offset_bucket = bytes_per_bucket * (bucket_number - m_begin);

        // Read MinHash buckets from files.
        spdlog::stopwatch sw_read;
        m_logger.info("[#{}] Read buckets from {} files", bucket_number, m_hfs.size());

        tbb::parallel_for_each(m_hfs.begin(), m_hfs.end(), [&](HashFile& hf) {
            size_t i = hf.start_index;
            
            // Open the hash file.
            std::ifstream ifs(hf.filename, std::ios::binary);
            if (ifs.fail()) {
                m_logger.critical("Failed to open the hash file: {}", hf.filename);
                throw MinHashLSHException();
            }

            // Read the buckets at #bucket_number.
            m_logger.trace("[#{}] Read {} buckets from {}", bucket_number, hf.num_items, hf.filename);
            for (size_t j = 0; j < hf.num_items; ++j) {
                m_items[i].i = i;
                ifs.seekg(32 + bytes_per_item * j + offset_bucket);
                ifs.read(reinterpret_cast<char*>(m_items[i].ptr()), bytes_per_bucket);
                ++i;
            }

            // Check whether the hash values are properly read.
            if (ifs.eof()) {
                m_logger.critical("Premature EOF of the hash file: {}", hf.filename);
                throw MinHashLSHException();
            }
            if (ifs.eof()) {
                m_logger.critical("Failed to read the content of the hash file: {}", hf.filename);
                throw MinHashLSHException();
            }             
        });
        m_logger.info("[#{}] Completed reading in {:.3f} seconds", bucket_number, sw_read);

        // Sort the buckets of items.
        spdlog::stopwatch sw_sort;
        m_logger.info("[#{}] Sort buckets", bucket_number);
        tbb::parallel_sort(m_items.begin(), m_items.end());
        //std::stable_sort(std::execution::par, m_items.begin(), m_items.end());
        m_logger.info("[#{}] Completed sorting in {:.3f} seconds", bucket_number, sw_sort);

        // Debugging the code.
        // for (auto it = m_items.begin(); it != m_items.end(); ++it) {
        //     std::cout << it->repr() << std::endl;
        // }

        // Count the number of inactive items.
        size_t num_active_before = std::count(m_flags.begin(), m_flags.end(), ' ');

        // Find duplicated items.
        spdlog::stopwatch sw_find;
        m_logger.info("[#{}] Find duplicates", bucket_number);
        for (auto cur = m_items.begin(); cur != m_items.end(); ) {
            // Find the next item that has a different bucket from the current one.
            auto next = cur + 1;
            while (next != m_items.end() && *cur == *next) {
                ++next;
            }
            // Mark a duplication flag for every item with the same bucket.
            for (++cur; cur != next; ++cur) {
                //std::cout << "Duplication: " << cur->i << std::endl;
                // Mark the item with a local duplicate flag (within this trial).
                m_flags[cur->i] = 'd';
            }
        }
        m_logger.info("[#{}] Completed finding duplicates in {:.3f} seconds", bucket_number, sw_find);

        // Count the number of active and detected (as duplicates) items.
        size_t num_active_after = std::count(m_flags.begin(), m_flags.end(), ' ');
        size_t num_detected = std::count(m_flags.begin(), m_flags.end(), 'd');

        // Save the index.
        if (save_index) {
            // Open an index file.
            IndexWriter writer;            
            std::string msg = writer.open(
                basename,
                bucket_number,
                bytes_per_bucket,
                m_num_items,
                m_num_items - num_detected
                );
            if (!msg.empty()) {
                m_logger.critical(msg);
                throw MinHashLSHException();
            }

            // Write the index to the file.
            m_logger.info("[#{}] Save the index to: {}", bucket_number, writer.m_filename);
            spdlog::stopwatch sw_save;
            for (auto it = m_items.begin(); it != m_items.end(); ++it) {
                // Write items that are non duplicates in this trial.
                if (m_flags[it->i] != 'd') {
                    writer.write_item(group, it->i, it->ptr());
                }
            }
            m_logger.info("[#{}] Completed saving the index in {:.3f} seconds", bucket_number, sw_save);
        }

        // Change local duplicate flags into global ones.
        for (auto it = m_flags.begin(); it != m_flags.end(); ++it) {
            *it = std::toupper(*it);    // 'd' -> 'D'.
        }

        // Report statistics.
        double active_ratio = 0 < m_num_items ? num_active_after / (double)m_num_items : 0.;
        double detection_ratio = 0 < m_num_items ? num_detected / (double)m_num_items : 0.;
        m_logger.info(
            "[#{}] Completed: {{"
            "\"num_active_before\": {}, "
            "\"num_detected\": {}, "
            "\"num_active_after\": {}, "
            "\"active_ratio\": {:.05f}, "
            "\"detection_ratio\": {:.05f}, "
            "\"time: {:.03f}"
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

    void run(const std::string& basename, size_t group, bool save_index = true)
    {
        spdlog::stopwatch sw;
        size_t num_active_before = std::count(m_flags.begin(), m_flags.end(), ' ');
        
        for (size_t bn = m_begin; bn < m_end; ++bn) {
            m_logger.info("Deduplication for #{}", bn);
            deduplicate_bucket(basename, group, bn, save_index);
        }
        
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
            "\"time\": {:.3f}"
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
};

int main(int argc, char *argv[])
{
    // Build a command-line parser.
    argparse::ArgumentParser program("doubri-dedup", __DOUBRI_VERSION__);
    program.add_description("Read MinHash buckets from files, deduplicate items, and build bucket indices.");
    program.add_argument("-g", "--group").metavar("N")
        .help("specifies a unique group order in the range of [0, 65535]")
        .nargs(1)
        .required()
        .scan<'d', int>();
    program.add_argument("-n", "--no-index")
        .help("does not save index files after deduplication")
        .flag();
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
    const int group = program.get<int>("group");
    const auto ignore_flag = program.get<bool>("ignore-flag");
    const auto no_index = program.get<bool>("no-index");
    const std::string flagfile = basename + std::string(".dup");
    const std::string logfile = basename + std::string(".log");
    const std::string srcfile = basename + std::string(".src");

    // Initialize the console logger.
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_level(spdlog::level::from_str(program.get<std::string>("log-level-console")));
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(logfile, true);
    file_sink->set_level(spdlog::level::from_str(program.get<std::string>("log-level-file")));
    spdlog::logger logger("doubri-dedup", {console_sink, file_sink});

    // Make sure that the group number is within 16 bits.
    if (group < 0 || 0xFFFF < group) {
        logger.critical("Group order must be in the range of [0, 65535]] {}", group);
        return 1;
    }

    // The deduplication object.
    MinHashLSH dedup(logger);

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

        // Write the group order.
        ofs << "#G " << group << std::endl;

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
    dedup.run(basename, group, !no_index);

    // Store the flag file.
    dedup.save_flag(flagfile);

    return 0;
}
