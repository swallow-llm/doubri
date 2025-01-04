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
#include "spdlog_util.hpp"

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

    bool read(std::vector<uint8_t>& bs)
    {
        bs.resize(m_bytes_per_item);
        m_ifs.read(
            reinterpret_cast<std::ifstream::char_type*>(&bs.front()),
            m_bytes_per_item
            );
        return !m_ifs.eof();
    }
};


struct Item {
    std::vector<uint8_t> m_bs;
    size_t m_k;

    Item(size_t k = 0) : m_k(k)
    {
    }

    virtual ~Item()
    {
    }

    /**
     * Spaceship operator for comparing buckets and group/item numbers.
     *
     * This defines dictionary order of byte streams of buckets. Because a
     * byte stream includes a bucket and group/item number in this order,
     * this function realizes ascending order of buckets, group numbers,
     * and item numbers.
     *
     *  @param  x   An item.
     *  @param  y   Another item.
     *  @return std::strong_ordering    The order of the two items.
     */
    friend auto operator<=>(const Item& x, const Item& y) -> std::strong_ordering
    {
        for (size_t i = 0; i < x.m_bs.size(); ++i) {
            int order = (int)x.m_bs[i] - (int)y.m_bs[i];
            if (order != 0) {
                return order <=> 0;
            }
        }
        return std::strong_ordering::equal;
        //std::strong_ordering order = x.m_bs <=> y.m_bs;
        //return order;
    }

    /**
     * Equality operator for comparing buckets.
     *
     * This defines the equality of two buckets. Unlike the operator <=>,
     * this function does not consider group and index numbers of the items
     * for equality, ignoring the last 8 bytes.
     *
     *  @param  x   An item.
     *  @param  y   Another item.
     *  @return bool    \c true when two items have the same bucket,
     *                  \c false otherwise.
     */
    friend bool operator==(const Item& x, const Item& y)
    {
        return std::memcmp(&x.m_bs.front(), &y.m_bs.front(), x.m_bs.size()-8) == 0;
    }

    std::string bucket() const
    {
        std::stringstream ss;
        for (size_t i = 0; i < m_bs.size()-8; ++i) {
            ss << std::hex << std::setfill('0') << std::setw(2) << (int)m_bs[i];
        }
        return ss.str();
    }

    size_t group() const
    {
        size_t v = 0;
        for (size_t i = m_bs.size()-8; i < m_bs.size()-6; ++i) {
            v <<= 8;
            v |= m_bs[i];
        }
        return v;
    }

    size_t index() const
    {
        size_t v = 0;
        for (size_t i = m_bs.size()-6; i < m_bs.size(); ++i) {
            v <<= 8;
            v |= m_bs[i];
        }
        return v;
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

    // Open source files.
    IndexReader readers[K];
    for (size_t k = 0; k < K; ++k) {
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
        } else {
            if (bytes_per_bucket != readers[k].m_bytes_per_bucket) {
                logger.critical("Inconsistent parameter, bytes_per_bucket: {}", bytes_per_bucket);
                return 2;
            }
            num_total_items += readers[k].m_num_total_items;
            num_active_items += readers[k].m_num_active_items;
        }
    }

    logger.info("bytes_per_bucket: {}", bytes_per_bucket);
    logger.info("num_total_items: {}", num_total_items);
    logger.info("num_active_items: {}", num_active_items);

    IndexWriter writer;
    writer.open(output, bucket_number, bytes_per_bucket, num_total_items, num_active_items);

    std::priority_queue<Item, std::vector<Item>, std::greater<Item> > pq;
    Item items[K];
    for (size_t k = 0; k < K; ++k) {
        items[k].m_k = k;
    }

    // Push the first item of each index to the priority queue.
    for (size_t k = 0; k < K; ++k) {
        if (readers[k].read(items[k].m_bs)) {
            pq.push(items[k]);
        }
    }

    //
    while (!pq.empty()) {
        auto top = pq.top();
        pq.pop();
        //writer.write_raw(top.get_reader()->ptr());
        std::cout << "+ " << top.bucket() << ' ' << top.group() << ' ' << top.index() << std::endl;
        while (!pq.empty() && pq.top() == top) {
            auto next = pq.top();
            pq.pop();
            size_t k = next.m_k;
            std::cout << "- " << next.bucket() << ' ' << next.group() << ' ' << next.index() << std::endl;
            if (readers[k].read(items[k].m_bs)) {
                pq.push(items[k]);
            }
        }
        size_t k = top.m_k;
        if (readers[k].read(items[k].m_bs)) {
            pq.push(items[k]);
        }
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
    program.add_argument("-l", "--log-console-level")
        .help("sets a log level for console")
        .default_value(std::string{"warning"})
        .choices("off", "trace", "debug", "info", "warning", "error", "critical")
        .nargs(1);
    program.add_argument("-L", "--log-file-level")
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
    console_sink->set_level(translate_log_level(program.get<std::string>("log-console-level")));
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(logfile, true);
    file_sink->set_level(translate_log_level(program.get<std::string>("log-file-level")));
    spdlog::logger logger("doubri-merge", {console_sink, file_sink});

    // Perform index merging.
    merge(logger, sources, output, begin, end);

    return 0;
}
