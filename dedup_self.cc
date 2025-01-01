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
#include <array>
#include <bit>
#include <concepts>
#include <cstdint>
#include <cstring>
#include <compare>
#include <execution>
#include <fstream>
#include <iomanip>
#include <ios>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <argparse/argparse.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/stopwatch.h>
#include <tbb/parallel_sort.h>
#include <tbb/parallel_for_each.h>

#include "common.h"

/**
 * An item for deduplication.
 *
 *  For space efficiency, this program allocates a (huge) array of buckets
 *  at a time rather than allocating a bucket buffer for each item. The
 *  static member \c s_buffer stores the pointer to the bucket array and
 *  the static member \c s_bytes_per_bucket presents the byte per bucket.
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
     * to mark the same item as duplicates across different trials of
     * deduplications with different bucket arrays. Calling std::sort()
     * will sort items in dictionary order of buckets and ascending order
     * of index numbers.
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
        ss << std::setfill('0') << std::setw(5) << i;
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
        m_logger.info("Allocate an array for buckets ({:.3f} MB)", size / 1000000.);
        m_buffer = new uint8_t[size];

        // Allocate an index for buckets.
        m_logger.info("Allocate an array for items");
        m_items.resize(m_num_items);

        // Allocate an array for non-duplicate flags.
        m_logger.info("Allocate an array for flags");
        m_flags.resize(m_num_items, ' ');

        // Set static members of Item to access the bucket array.
        Item::s_buffer = m_buffer;
        Item::s_bytes_per_bucket = m_bytes_per_hash * m_num_hash_values;
    }

    void load_flag(const std::string& filename)
    {
        m_logger.info("Load flags from a file: {}", filename);

        // Open the flag file.
        std::ifstream ifs(filename);
        if (ifs.fail()) {
            m_logger.critical("Failed to open a flag file");
            throw MinHashLSHException();
        }

        // Check the size of the flag file.
        ifs.seekg(0, std::ios::end);
        auto filesize = ifs.tellg();
        if (filesize != m_num_items) {
            m_logger.critical("Number of elements is diffeerent");
            throw MinHashLSHException();
        }
        ifs.seekg(0, std::ios::beg);

        // Read the flags.
        ifs.read(reinterpret_cast<std::ifstream::char_type*>(&m_flags.front()), filesize);

        // Check whether the flags are properly read.
        if (ifs.eof() || ifs.fail()) {
            m_logger.critical("Failed to read the content of the flag file");
            throw MinHashLSHException();
        }
    }

    void save_flag(const std::string& filename)
    {
        m_logger.info("Save flags to a file: {}", filename);

        // Open the flag file.
        std::ofstream ofs(filename);
        if (ofs.fail()) {
            m_logger.critical("Failed to open a flag file");
            throw MinHashLSHException();
        }

        // Write the flags.
        ofs.write(reinterpret_cast<std::ifstream::char_type*>(&m_flags.front()), m_flags.size());

        // Check whether the flags are properly read.
        if (ofs.fail()) {
            m_logger.critical("Failed to write the flags to a file.");
            throw MinHashLSHException();
        }
    }

    void deduplicate_bucket(size_t bucket_number, const std::string& basename, bool save_index = true, bool parallel = false)
    {
        m_logger.info("Start deduplication for #{}", bucket_number);

        const size_t bytes_per_bucket = m_bytes_per_hash * m_num_hash_values;
        const size_t bytes_per_item = bytes_per_bucket * (m_end - m_begin);
        const size_t offset_bucket = bytes_per_bucket * (bucket_number - m_begin);

        // Read MinHash buckets from files.
        spdlog::stopwatch sw_read;
        m_logger.info("Read buckets #{} from {} files", bucket_number, m_hfs.size());

        tbb::parallel_for_each(m_hfs.begin(), m_hfs.end(), [&](HashFile& hf) {
            size_t i = hf.start_index;
            
            // Open the hash file.
            std::ifstream ifs(hf.filename, std::ios::binary);
            if (ifs.fail()) {
                m_logger.critical("Failed to open the hash file: {}", hf.filename);
                throw MinHashLSHException();
            }

            // Read the buckets at #bucket_number.
            m_logger.trace("Read {} buckets from {} for #{}", hf.num_items, hf.filename, bucket_number);
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
        m_logger.info("Completed reading in {:.3f} seconds", sw_read);

        // Sort the buckets of items.
        spdlog::stopwatch sw_sort;
        if (parallel) {
            m_logger.info("Sort buckets (multi-thread)");
            tbb::parallel_sort(m_items.begin(), m_items.end());
            //std::stable_sort(std::execution::par, m_items.begin(), m_items.end());
        } else {
            m_logger.info("Sort buckets (single-thread)");
            std::sort(m_items.begin(), m_items.end());
        }
        m_logger.info("Completed sorting in {:.3f} seconds", sw_sort);

        // Debugging the code.
        // for (auto it = m_items.begin(); it != m_items.end(); ++it) {
        //     std::cout << it->repr() << std::endl;
        // }

        // Count the number of inactive items.
        size_t num_active_before = std::count(m_flags.begin(), m_flags.end(), ' ');

        // Find duplicated items.
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

        // Count the number of active and detected (as duplicates) items.
        size_t num_active_after = std::count(m_flags.begin(), m_flags.end(), ' ');
        size_t num_detected = std::count(m_flags.begin(), m_flags.end(), 'd');

        // Save the index.
        if (save_index) {
            // Open the index file.
            std::stringstream ss;
            ss << basename << std::setfill('0') << std::setw(5) << bucket_number;
            const std::string filename = ss.str();
            std::ofstream ofs(filename);
            if (ofs.fail()) {
                m_logger.critical("Failed to open an index file: {}", filename);
                throw MinHashLSHException();
            }

            // Write the index to the file.
            for (auto it = m_items.begin(); it != m_items.end(); ++it) {
                // Write items that are non duplicates in this trial.
                if (m_flags[it->i] != 'd') {
                    write_value<uint64_t>(ofs, it->i);
                    ofs.write(reinterpret_cast<const char*>(it->ptr()), bytes_per_bucket);
                }
            }
        }

        // Change local duplicate flags into global ones.
        for (auto it = m_flags.begin(); it != m_flags.end(); ++it) {
            *it = std::toupper(*it);    // 'd' -> 'D'.
        }

        // Report statistics.
        double active_ratio = 0 < m_num_items ? num_active_after / (double)m_num_items : 0.;
        double detection_ratio = 0 < m_num_items ? num_detected / (double)m_num_items : 0.;
        m_logger.info(
            "Completed for #{}: {{"
            "\"num_active_before\": {}, "
            "\"num_detected\": {}, "
            "\"num_active_after\": {}, "
            "\"active_ratio\": {:.05f}, "
            "\"detection_ratio\": {:.05f}"
            "}}",
            bucket_number,
            num_active_before,
            num_detected,
            num_active_after,
            active_ratio,
            detection_ratio
            );
    }

    void run(const std::string& basename, bool save_index = true, bool parallel = false)
    {
        spdlog::stopwatch sw;
        size_t num_active_before = std::count(m_flags.begin(), m_flags.end(), ' ');
        
        for (size_t bn = m_begin; bn < m_end; ++bn) {
            deduplicate_bucket(bn, basename, save_index, parallel);
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

auto translate_log_level(const std::string& level)
{
    if (level == "off") {
        return spdlog::level::off;
    } else if (level == "trace") {
        return spdlog::level::trace;
    } else if (level == "debug") {
        return spdlog::level::debug;
    } else if (level == "info") {
        return spdlog::level::info;
    } else if (level == "warning") {
        return spdlog::level::warn;
    } else if (level == "error") {
        return spdlog::level::err;
    } else if (level == "critical") {
        return spdlog::level::critical;
    } else {
        std::string msg = std::string("Unknown log level: ") + level;
        throw std::invalid_argument(msg);
    }
}

int main(int argc, char *argv[])
{
    // Build a command-line parser.
    argparse::ArgumentParser program("doubri-self", __DOUBRI_VERSION__);
    program.add_description("Read MinHash buckets from files, deduplicate items, and build bucket indices.");
    program.add_argument("-p", "--parallel")
        .help("uses multi-thread sorting for speed up")
        .flag();
    program.add_argument("-f", "--ignore-flag")
        .help("ignores existing flags to cold-start deduplication")
        .flag();
    program.add_argument("-n", "--no-index")
        .help("does not save index files after deduplication")
        .flag();
    program.add_argument("-l", "--log-console-level")
        .help("sets a log level for console")
        .default_value(std::string{"warning"})
        .choices("off", "trace", "debug", "info", "warning", "error", "critical")
        .nargs(1);
    program.add_argument("-L", "--log-file-level")
        .help("sets a log level for file logging ({BASENAME}.log.txt)")
        .default_value(std::string{"off"})
        .choices("off", "trace", "debug", "info", "warning", "error", "critical")
        .nargs(1);
    program.add_argument("basename").metavar("BASENAME")
        .help("basename for index (.#####) and flag (.dup) files");

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
    const auto ignore_flag = program.get<bool>("ignore-flag");
    const auto no_index = program.get<bool>("no-index");
    const auto parallel = program.get<bool>("parallel");
    const std::string flagfile = basename + std::string(".dup");
    const std::string logfile = basename + std::string(".log.txt");

    // Initialize the console logger.
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_level(translate_log_level(program.get<std::string>("log-console-level")));
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(logfile, true);
    file_sink->set_level(translate_log_level(program.get<std::string>("log-file-level")));
    spdlog::logger logger("doubri-self", {console_sink, file_sink});

    // The deduplication object.
    MinHashLSH dedup(logger);

    // One MinHash file per line.
    for (;;) {
        // Read a line from STDIN.
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

    // Load the flag file.
    if (!ignore_flag) {
        // Load the flag file if it exists.
        std::ifstream ifs(flagfile);
        if (!ifs.fail()) {
            ifs.close();
            dedup.load_flag(flagfile);
        } else {
            logger.info("Flag file does not exists: {}", flagfile);
        }
    } else {
        logger.info("The user instructed to ignore a flag file");
    }

    // Perform deduplication.
    dedup.run(basename, !no_index, parallel);

    // Store the flag file.
    dedup.save_flag(flagfile);

    return 0;
}
