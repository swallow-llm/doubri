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

/**
 * An item in an index (a list of sorted buckets with their IDs).
 */
struct Item {
    /// Item ID.
    uint64_t id;
    /// Bucket byte-stream.
    uint8_t bucket[1];  // Flexible array member (variable size).
};

class ItemArray {
public:
    /// Whether this object should manage the memory for m_items.
    bool m_managed{false};
    /// Index number of the first item. 
    size_t m_begin{0};
    /// Index number of the next to the last item.
    size_t m_end{0};
    /// Number of items in the array.
    size_t m_length{0}:
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
    ItemArray(uint8_t* items, size_t length, size_t bytes_per_item, size_t begin, size_t end) :
        m_managed(false), m_begin(begin), m_end(end), m_length(length),
        m_bytes_per_item(bytes_per_item), m_items(items)
    {
    }

    /**
     * Constructs by copying the array of another ItemArray instance.
     *  @param  src     Another ItemArray instance.
     */
    ItemArray(const ItemArray& src) :
        m_managed(true), m_begin(0), m_end(src.end - src.begin), m_length(src.end - src.begin),
        m_bytes_per_item(src.m_bytes_per_item)
    {
        size_t size = m_bytes_per_item * m_length;
        m_items = new uint8_t[size];
        std::memcpy(m_items, &src.m_items[src.m_bytes_per_item * src.begin], size);
    }

    virtual ~ItemArray()
    {
        if (m_managed && m_items != nullptr) {
            delete[] m_items;
        }
        m_items = nullptr;
        m_begin = 0;
        m_end = 0;
    }

    Item& operator[](size_t i)
    {
        return *reinterpret_cast<Item*>(m_items + m_bytes_per_item * (m_begin + i));
    }

    const Item& operator[](size_t i) const
    {
        return *reinterpret_cast<const Item*>(m_items + m_bytes_per_item * (m_begin + i));
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

void merge(ItemArray A[], size_t left, size_t mid, size_t right)
{
    ItemArray L(A[left]);
    ItemArray R(A[mid]);

    ItemArray& M = A[left];
    size_t i = 0, j = 0, k = 0;
    size_t bytes_per_bucket = L.bytes_per_item();
    size_t bytes_per_item = bytes_per_bucket - sizeof(uint64_t);
    while (i < L.length() && j < R.length()) {
        int cmp = std::memcmp(L[i].bucket, R[j].bucket, bytes_per_bucket);
        if (cmp < 0) {
            std::memcpy(&M[k++], &L[i++], bytes_per_item);
        } else if (cmp > 0) {
            std::memcpy(&M[k++], &R[j++], bytes_per_item);
        } else {
            std::memcpy(&M[k++], &L[i++], bytes_per_item);
            ++j;
        }
    }

    while (i < L.length()) {
        std::memcpy(&M[k++], &L[i++], bytes_per_item);
    }
    while (j < R.length()) {
        std::memcpy(&M[k++], &R[j++], bytes_per_item);
    }

    M.set_number_of_items(k);
}

void unique(ItemArray A[], size_t left, size_t right)
{
    if (left + 1 < right) {
        size_t mid = (left + right) / 2;
        unique(A, left, mid);
        unique(A, mid, right);
        merge(A, left, mid, right);
    }
}

int merge_index(
    spdlog::logger& logger,
    const std::vector<std::string>& sources,
    const std::string& output,
    int start,
    int end
    )
{
    const size_t G = sources.size();

    for (int bn = start; bn < end; bn++) {
        for (size_t split = SPLIT_BEGIN; split < SPLIT_END; ++split) {
            size_t num_items[G];
            size_t num_active_items = 0;
            size_t num_total_items = 0;
            size_t bytes_per_bucket = 0;
            for (size_t g = 0; g < G; ++g) {
                IndexReader reader;
                reader.open(sources[g], bn, (uint8_t)split, true);
                num_items[g] = reader.m_num_active_items;
                num_active_items += reader.m_num_active_items;
                num_total_items += reader.m_num_total_items;
                if (g == 0) {
                    bytes_per_bucket = reader.m_bytes_per_bucket;
                } else {
                    if (bytes_per_bucket != reader.m_bytes_per_bucket) {
                        // Inconsistent bytes per bucket across groups.
                    }
                }
            }

            const size_t bytes_per_item = sizeof(uint64_t) + bytes_per_bucket;
            uint8_t *buffer = uint8_t[bytes_per_item];
            ItemArray indices[G];
            
        }
    }
    return 0;
}

int main(int argc, char *argv[])
{
    // Build a command-line parser.
    argparse::ArgumentParser program("doubri-merge", __DOUBRI_VERSION__);
    program.add_description("Merge bucket indices to deduplicate items across different groups.");
    program.add_argument("-s", "--start").metavar("START")
        .help("start number of buckets")
        .nargs(1)
        .default_value(0)
        .scan<'d', int>();
    program.add_argument("-r", "--end").metavar("END")
        .help("end number of buckets (number of buckets when START = 0)")
        .nargs(1)
        .default_value(40)
        .scan<'d', int>();
    program.add_argument("-o", "--output").metavar("OUTPUT")
        .help("basename for index ({OUTPUT}.idx.#####) and flag ({OUTPUT}.dup) files")
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
    const auto begin = program.get<int>("start");
    const auto end = program.get<int>("end");
    const auto output = program.get<std::string>("output");
    const auto sources = program.get<std::vector<std::string>>("sources");
    const std::string flagfile = output + std::string(".dup");
    const std::string logfile = output + std::string(".log.txt");

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
    return merge_index(logger, sources, output, begin, end);
}

