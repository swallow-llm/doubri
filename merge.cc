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
#include <queue>

#include <argparse/argparse.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/stopwatch.h>

#include "common.h"
#include "index.hpp"

/**
 * An element (bucket, group, and item) of indices (sorted lists).
 *
 *  This class implements strong ordering and equality of index elements so
 *  that we can merge K sorted lists of elements by using a priority queue.
 *  This class does not hold a byte sequence representing a bucket, group,
 *  and item but only stores a pointer to a reader object \c m_reader. This
 *  prevents copying a byte sequence itself when insering an element into a
 *  priority queue.
 */
struct Element {
    IndexReader* reader{nullptr};

    /**
     * Spaceship operator for comparing elements (bucket, group, and item).
     *
     * This defines dictionary order of buckets, groups, and items in this
     * priority. Because a byte sequence includes a bucket, group, and item
     * in this order, we can simply compare the two vectors associated with
     * the items.
     *
     *  @param  x   An element.
     *  @param  y   Another element.
     *  @return std::strong_ordering    The order of the two elements.
     */
    friend auto operator<=>(const Element& x, const Element& y) -> std::strong_ordering
    {
        return x.reader->vec() <=> y.reader->vec();
    }

    /**
     * Equality operator for comparing elements (bucket, group, and item).
     *
     * This defines the equality of two buckets. Unlike the operator <=>,
     * this function does not consider group and item numbers of the elements
     * for equality, ignoring the last 8 bytes.
     *
     *  @param  x   An item.
     *  @param  y   Another item.
     *  @return bool    \c true when two items have the same bucket,
     *                  \c false otherwise.
     */
    friend bool operator==(const Element& x, const Element& y)
    {
        return std::memcmp(
            x.reader->ptr(),
            y.reader->ptr(),
            x.reader->bytes_per_bucket()
            ) == 0;
    }
};


int merge_index(
    spdlog::logger& logger,
    const std::vector<std::string>& sources,
    const std::string& output,
    size_t bucket_number
)
{
    const size_t K = sources.size();
    size_t bytes_per_bucket = 0;
    size_t num_total_items = 0;
    size_t num_active_items = 0;

    // Open index files through K reader objects.
    IndexReader readers[K];
    for (size_t k = 0; k < K; ++k) {
        // Open an index file.
        logger.trace("Open an index file: {}", sources[k]);
        std::string msg = readers[k].open(sources[k], bucket_number);
        if (!msg.empty()) {
            logger.critical(msg);
            return 1;
        }

        // Retrieve parameters from the header of the index file.
        if (k == 0) {
            bytes_per_bucket = readers[k].m_bytes_per_bucket;
            num_total_items = readers[k].m_num_total_items;
            num_active_items = readers[k].m_num_active_items;
            logger.info("bytes_per_bucket: {}", bytes_per_bucket);
            logger.info("num_total_items: {}", num_total_items);
            logger.info("num_active_items: {}", num_active_items);
        } else {
            if (bytes_per_bucket != readers[k].m_bytes_per_bucket) {
                logger.critical("Inconsistent parameter, bytes_per_bucket: {}", readers[k].m_bytes_per_bucket);
                return 2;
            }
            num_total_items += readers[k].m_num_total_items;
            num_active_items += readers[k].m_num_active_items;
        }
        if (readers[k].m_bucket_number != bucket_number) {
            logger.critical("Inconsistent bucket number: {}", readers[k].m_bucket_number);
            return 2;
        }
    }

    // Writer for the index that merges the source indices.
    IndexWriter writer;
    if (!output.empty()) {
        std::string msg = writer.open(
            output,
            bucket_number,
            bytes_per_bucket,
            num_total_items,
            num_active_items);
        if (!msg.empty()) {
            logger.critical(msg);
            return 3;
        }
    }

    // The priority queue to select elements in ascending order.
    // Note that we need to specify std::greater for ascending order.
    std::priority_queue<Element, std::vector<Element>, std::greater<Element> > pq;

    // Frontier elements from source indices.
    Element items[sources.size()];
    for (size_t k = 0; k < K; ++k) {
        // Associate the k-th element with the k-th reader.
        items[k].reader = &readers[k];
    }

    // Push the first element of each index to the priority queue.
    for (size_t k = 0; k < K; ++k) {
        if (items[k].reader->next()) {
            pq.push(items[k]);
        }
    }

    // Merge the source indices.
    num_active_items = 0;
    while (!pq.empty()) {
        // Pop the minimum element.
        auto top = pq.top();
        pq.pop();
        ++num_active_items;

        // Write the element to the output index if necessary.
        if (!output.empty()) {
            writer.write_raw(top.reader->ptr());
        }

        std::cout
            << "+ "
            << top.reader->bucket() << " "
            << top.reader->group() << " "
            << top.reader->item() << std::endl;

        // Skip elements with the identical bucket to the minimum element.
        while (!pq.empty() && pq.top() == top) {
            // Pop the element (this is a duplicate of the minimum element)
            auto next = pq.top();
            pq.pop();

            std::cout
                << "+ "
                << next.reader->bucket() << " "
                << next.reader->group() << " "
                << next.reader->item() << std::endl;

            // Read the next element from the index whose frontier element has
            // just been recognized as a duplicate.
            if (next.reader->next()) {
                pq.push(next);
            }
        }

        // Read the next element from the index whose frontier element was the
        // minimum element (non-duplicate).
        if (top.reader->next()) {
            pq.push(top);
        }
    }

    if (!output.empty()) {
        writer.update_num_active_items(num_active_items);
    }

    return 0;
}

int merge(
    spdlog::logger& logger,
    const std::vector<std::string>& sources,
    const std::string& output,
    size_t begin,
    size_t end
)
{
    for (size_t bn = begin; bn < end; ++bn) {
        merge_index(logger, sources, output, bn);
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
    console_sink->set_level(spdlog::level::from_str(program.get<std::string>("log-level-console")));
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(logfile, true);
    file_sink->set_level(spdlog::level::from_str(program.get<std::string>("log-level-file")));
    spdlog::logger logger("doubri-merge", {console_sink, file_sink});

    // Perform index merging.
    merge(logger, sources, output, begin, end);

    return 0;
}

