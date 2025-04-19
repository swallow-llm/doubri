/*
    Merge bucket indices to deduplicate items across different groups.

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

#include <cstdint>
#include <functional>
#include <iostream>

#include <argparse/argparse.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/stopwatch.h>

#include "common.h"
#include "index.hpp"
#include "flag.hpp"

//#define DEBUG_MERGE    1

class ItemArray {
public:
    /// Whether this object should manage the memory for m_items.
    bool m_managed{false};
    /// Index number of the first item. 
    size_t m_begin{0};
    /// Index number of the next to the last item.
    size_t m_end{0};
    /// Number of items in the array.
    size_t m_length{0};
    /// Number of bytes per item.
    size_t m_bytes_per_item{0};
    /// The item array.
    uint8_t* m_items{nullptr};

public:
    /**
     * Constructs by default.
     */
    ItemArray()
    {
    }

    /**
     * Constructs with an external array.
     *  @param  items   The pointer to the external array.
     *  @param  length  The number of items in the external array.
     *  @param  bytes_per_item  The number of bytes per item.
     *  @param  begin   The index number of the first item.
     *  @param  end     The index number of the next to the last item.
     */
    ItemArray(uint8_t* items, size_t length, size_t bytes_per_item, size_t begin, size_t end)
    {
        init(items, length, bytes_per_item, begin, end);
    }

    /**
     * Copies from an ItemArray instance.
     *  @param  src     The source array.
     */
    ItemArray(const ItemArray& src)
    {
        m_managed = true;
        m_begin = 0;
        m_end = src.m_end - src.m_begin;
        m_length = m_end;
        m_bytes_per_item = src.m_bytes_per_item;

        size_t size = m_bytes_per_item * m_length;
        m_items = new uint8_t[size];
        std::memcpy(m_items, &src.m_items[src.m_bytes_per_item * src.m_begin], size);
    }

    /**
     * Initializes with an external array.
     *  @param  items   The pointer to the external array.
     *  @param  length  The number of items in the external array.
     *  @param  bytes_per_item  The number of bytes per item.
     *  @param  begin   The index number of the first item.
     *  @param  end     The index number of the next to the last item.
     */
    void init(uint8_t* items, size_t length, size_t bytes_per_item, size_t begin, size_t end)
    {
        m_managed = false;
        m_begin = begin;
        m_end = end;
        m_length = length;
        m_bytes_per_item = bytes_per_item;
        m_items = items;
    }

    /**
     * Destructs the instance.
     */
    virtual ~ItemArray()
    {
        if (m_managed && m_items != nullptr) {
            delete[] m_items;
        }
        m_begin = 0;
        m_end = 0;
        m_length = 0;
        m_bytes_per_item = 0;
        m_items = nullptr;
    }

    /**
     * Obtains the pointer to the item.
     *  @param  i   The index number of the item.
     *  @return     The pointer to the item.
     */
    uint8_t* operator[](size_t i)
    {
        return m_items + m_bytes_per_item * (m_begin + i);
    }

    /**
     * Obtains the pointer to the item.
     *  @param  i   The index number of the item.
     *  @return     The pointer to the item.
     */
    const uint8_t* operator[](size_t i) const
    {
        return m_items + m_bytes_per_item * (m_begin + i);
    }

    size_t size() const
    {
        return m_end - m_begin;
    }

    size_t length() const
    {
        return m_length;
    }

    size_t bytes_per_item() const
    {
        return m_bytes_per_item;
    }

    void set_number_of_items(size_t n)
    {
        m_end = m_begin + n;
    }
};

template <bool reverse>
size_t merge(ItemArray A[], size_t left, size_t mid, size_t right, std::vector<char> flags[], spdlog::logger& logger)
{
    size_t num_deleted = 0;

    // Copy left and right item arrays to L and R.
    ItemArray L = A[left];
    ItemArray R = A[mid];

    // The item array to which items in the two arrays are merged.
    ItemArray& M = A[left];

    // Merge the two item arrays L and R (similarly to merge sort).
    size_t i = 0, j = 0, k = 0;
    size_t bytes_per_item = L.bytes_per_item();
    size_t bytes_per_bucket = bytes_per_item - sizeof(uint64_t);
    while (i < L.length() && j < R.length()) {
        const uint8_t* lb = L[i] + sizeof(uint64_t);
        const uint8_t* rb = R[j] + sizeof(uint64_t);
        int cmp = std::memcmp(lb, rb, bytes_per_bucket);
        if (cmp < 0) {
            std::memcpy(M[k++], L[i++], bytes_per_item);
        } else if (cmp > 0) {
            std::memcpy(M[k++], R[j++], bytes_per_item);
        } else {
            if constexpr (reverse) {
#ifdef  DEBUG_MERGE
                logger.info("Merge: {} {}", IndexReader::repr_id(R[j]), IndexReader::repr_id(L[i]));
#endif/*DEBUG_MERGE*/
                std::memcpy(M[k++], R[j++], bytes_per_item);
                flags[IndexReader::group_number(L[i])][IndexReader::item_number(L[i])] = 'D';
                ++i;
            } else {
#ifdef  DEBUG_MERGE
                logger.info("Merge: {} {}", IndexReader::repr_id(L[i]), IndexReader::repr_id(R[j]));
#endif/*DEBUG_MERGE*/
                std::memcpy(M[k++], L[i++], bytes_per_item);
                flags[IndexReader::group_number(R[j])][IndexReader::item_number(R[j])] = 'D';
                ++j;
            }
            ++num_deleted;
        }
    }

    // Copy remaining items in L to M.
    while (i < L.length()) {
        std::memcpy(M[k++], L[i++], bytes_per_item);
    }
    // Copy remaining items in R to M.
    while (j < R.length()) {
        std::memcpy(M[k++], R[j++], bytes_per_item);
    }

    // Set the number of items in the merged array.
    M.set_number_of_items(k);
    return num_deleted;
}

template <bool reverse>
size_t unique(ItemArray A[], size_t left, size_t right, std::vector<char> flags[], spdlog::logger& logger)
{
    size_t num_deleted = 0;
    if (left + 1 < right) {
        size_t mid = (left + right) / 2;
        num_deleted += unique<reverse>(A, left, mid, flags, logger);
        num_deleted += unique<reverse>(A, mid, right, flags, logger);
        num_deleted += merge<reverse>(A, left, mid, right, flags, logger);
    }
    return num_deleted;
}

int merge_index(
    spdlog::logger& logger,
    const std::vector<std::string>& sources,
    const std::string& output,
    const bool reverse,
    const int start,
    const int end
    )
{
    spdlog::stopwatch sw_merge;
    const size_t G = sources.size();

    logger.info("reverse: {}", reverse);
    logger.info("begin: {}", start);
    logger.info("end: {}", end);
    for (size_t g = 0; g < G; ++g) {
        logger.info("sources[{}]: {}", g, sources[g]);
    }

    // Load the flag files.
    std::vector<char> flags[G];
    size_t num_active_start = 0, num_overall_total_items = 0;
    for (size_t g = 0; g < G; ++g) {
        std::string filename = sources[g] + std::string(".dup");
        std::string msg = flag_load(filename, flags[g]);
        if (!msg.empty()) {
            logger.critical("{}", msg);
            return 1;
        }
        size_t num_active = std::count(flags[g].begin(), flags[g].end(), ' ');
        size_t num_total = flags[g].size();
        num_active_start += num_active;
        num_overall_total_items += num_total;
        logger.info("{} / {} items in flag file: {}", num_active, num_total, filename);
    }

    logger.info("Merge {} / {} items in {} groups", num_active_start, num_overall_total_items, G);

    // Loop for each bucket.
    size_t num_active_before = num_active_start;
    for (int bn = start; bn < end; bn++) {
        spdlog::stopwatch sw_bucket;

        // Loop for each split.
        for (size_t split = SPLIT_BEGIN; split < SPLIT_END; ++split) {
            spdlog::stopwatch sw;

            // Find the total number of items of all sources.
            size_t begins[G];
            size_t num_active_items = 0, num_total_items = 0;
            size_t bytes_per_bucket = 0;
            for (size_t g = 0; g < G; ++g) {
                IndexReader reader;
                std::string msg = reader.open(sources[g], bn, (uint8_t)split, true);
                if (!msg.empty()) {
                    logger.critical("{}", msg);
                    return 1;
                }

                begins[g] = num_active_items;
                num_active_items += reader.m_num_active_items;
                num_total_items += reader.m_num_total_items;

                // Check the consistency of bytes_per_item
                if (g == 0) {
                    bytes_per_bucket = reader.m_bytes_per_bucket;
                } else {
                    if (bytes_per_bucket != reader.m_bytes_per_bucket) {
                        logger.critical("The number of bytes per bucket {} is inconsistent with {}: {}", reader.m_bytes_per_bucket, bytes_per_bucket, sources[g]);
                        return 1;
                    }
                }
            }

            logger.info("[#{}:{:02x}] {} / {} items", bn, split, num_active_items, num_total_items);

            // Allocate a big item array that concatenates the arrays of all sources.
            const size_t bytes_per_item = sizeof(uint64_t) + bytes_per_bucket;
            const size_t size = bytes_per_item * num_active_items;
            uint8_t *buffer = new uint8_t[size];
            logger.info("[#{}:{:02x}] Allocate an array for index items ({:.3f} MB)", bn, split, size / (double)1e6);

            // Read item arrays from all sources.
            ItemArray indices[G];
            for (size_t g = 0; g < G; ++g) {
                // Read the item array of this source.
                IndexReader reader;
                std::string msg = reader.open(sources[g], bn, (uint8_t)split, true);
                if (!msg.empty()) {
                    logger.critical("{}", msg);
                    return 1;
                }
                if (!reader.read_all(buffer + bytes_per_item * begins[g])) {
                    logger.critical("Failed to read an item index from {}", sources[g]);
                    return 1;
                }

                // Associate the item array with the ItemArray object.
                ItemArray& index = indices[g];
                index.init(
                    buffer,
                    num_active_items,
                    bytes_per_item,
                    begins[g],
                    begins[g] + reader.m_num_active_items
                    );

                // Set the group number to all items.
                for (size_t i = 0; i < index.size(); ++i) {
                    IndexWriter::set_group(index[i], g);
                }
            }

            // Merge item arrays.
            size_t num_deleted = 0;
            if (reverse) {
                num_deleted = unique<true>(indices, 0, G, flags, logger);
            } else {
                num_deleted = unique<false>(indices, 0, G, flags, logger);
            }
            logger.info("[#{}:{:02x}] Completed merging in {:.3f} seconds", bn, split, sw);

            // Free the big array.
            delete[] buffer;
        }

        // Count the number of active items.
        size_t num_active_after = 0;
        for (size_t g = 0; g < G; ++g) {
            num_active_after += std::count(flags[g].begin(), flags[g].end(), ' ');
        }

        // Report statistics.
        double active_ratio = 0 < num_overall_total_items ? num_active_after / (double)num_overall_total_items : 0.;
        logger.info(
            "[#{}] Merge completed: {{"
            "\"num_active_before\": {}, "
            "\"num_active_after\": {}, "
            "\"active_ratio\": {:.05f}, "
            "\"time\": {:.03f}"
            "}}",
            bn,
            num_active_before,
            num_active_after,
            active_ratio,
            sw_bucket
            );
        num_active_before = num_active_after;
    }

    // Save the flag files.
    size_t num_active_after = 0;
    for (size_t g = 0; g < G; ++g) {
        std::string filename = sources[g] + std::string(".dup.merge");
        std::string msg = flag_save(filename, flags[g]);
        if (!msg.empty()) {
            logger.critical("{}", msg);
            return 1;
        }
        logger.info("Save flags to a file: {}", filename);
        num_active_after += std::count(flags[g].begin(), flags[g].end(), ' ');
    }

    double active_ratio = 0 < num_overall_total_items ? num_active_after / (double)num_overall_total_items : 0.;
    logger.info(
        "Result: {{"
        "\"num_active_before\": {}, "
        "\"num_active_after\": {}, "
        "\"active_ratio\": {:.05f}, "
        "\"time\": {:.03f}"
        "}}",
        num_active_start,
        num_active_after,
        active_ratio,
        sw_merge
        );
    return 0;
}

int main(int argc, char *argv[])
{
    // Build a command-line parser.
    argparse::ArgumentParser program("doubri-merge", __DOUBRI_VERSION__);
    program.add_description("Merge bucket indices to deduplicate items across different groups.");
    program.add_argument("-r", "--reverse")
        .help("keep newer duplicated items (older items, by default)")
        .default_value(false)
        .flag();
    program.add_argument("-s", "--start").metavar("START")
        .help("start number of buckets")
        .nargs(1)
        .default_value(0)
        .scan<'d', int>();
    program.add_argument("-e", "--end").metavar("END")
        .help("end number of buckets (number of buckets when START = 0)")
        .nargs(1)
        .default_value(40)
        .scan<'d', int>();
    program.add_argument("-o", "--output").metavar("OUTPUT")
        .help("basename for the log file ({OUTPUT}.log)")
        .nargs(1)
        .required();
    program.add_argument("-l", "--log-level-console")
        .help("sets a log level for console")
        .default_value(std::string{"warning"})
        .choices("off", "trace", "debug", "info", "warning", "error", "critical")
        .nargs(1);
    program.add_argument("-L", "--log-level-file")
        .help("sets a log level for file logging ({OUTPUT}.log.txt)")
        .default_value(std::string{"off"})
        .choices("off", "trace", "debug", "info", "warning", "error", "critical")
        .nargs(1);
    program.add_argument("sources")
        .help("basenames for index (.idx.#####) and flag (.dup) files")
        .remaining();

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
    const auto reverse = program.get<bool>("reverse");
    const auto begin = program.get<int>("start");
    const auto end = program.get<int>("end");
    const auto output = program.get<std::string>("output");
    const auto sources = program.get<std::vector<std::string>>("sources");
    const std::string logfile = output + std::string(".log");

    // Initialize the console logger.
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto console_log_level = spdlog::level::from_str(program.get<std::string>("log-level-console"));
    console_sink->set_level(console_log_level);

    // Initialize the file logger.
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(logfile, true);
    auto file_log_level = spdlog::level::from_str(program.get<std::string>("log-level-file"));
    file_sink->set_level(file_log_level);

    // Create the logger that integrates console and file loggers.
    spdlog::logger logger("doubri-merge", {console_sink, file_sink});
    logger.flush_on(file_log_level);

    // Perform index merging.
    return merge_index(logger, sources, output, reverse, begin, end);
}
