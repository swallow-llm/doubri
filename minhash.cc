/*
    MinHash generator.

Copyright (c) 2023-2024, Naoaki Okazaki

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

#include <bit>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>
#include <utf8.h>
#include <nlohmann/json.hpp>
#include <argparse/argparse.hpp>
#include "common.h"
#include "minhash.h"

using json = nlohmann::json;

int main(int argc, char *argv[])
{
    uint32_t num_items = 0;
    const uint32_t zero = 0;
    std::istream& is = std::cin;
    std::ostream& os = std::cout;
    std::ostream& es = std::cerr;

    argparse::ArgumentParser program("doubri-minhash", __DOUBRI_VERSION__);
    program.add_description("Read text (in JSONL format) from STDIN and compute MinHash buckets.");
    program.add_argument("-n", "--ngram").metavar("N")
        .help("number of letters of an n-gram")
        .default_value(5)
        .nargs(1);
    program.add_argument("-b", "--bucket").metavar("HASHNUM")
        .help("number of hash values per bucket")
        .default_value(20)
        .nargs(1);
    program.add_argument("-s", "--start").metavar("START")
        .help("start number of buckets")
        .default_value(0)
        .nargs(1);
    program.add_argument("-r", "--end").metavar("END")
        .help("end number of buckets (number of buckets when START = 0)")
        .default_value(40)
        .nargs(1);
    program.add_argument("-t", "--text").metavar("TEXT")
        .help("text field in JSON")
        .default_value("text")
        .nargs(1);
    program.add_argument("filename").metavar("FILENAME")
        .help("filename where MinHash buckets will be stored");

    // Parse the command-line arguments.
    try {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& e) {
        es << e.what() << std::endl;
        es << program;
        return 1;
    }

    // Retrieve parameters.
    const auto n = program.get<int>("ngram");
    const uint32_t bytes_per_hash = 4;
    const uint32_t num_hash_values = (uint32_t)program.get<int>("bucket");
    const uint32_t begin = program.get<int>("start");
    const uint32_t end = program.get<int>("end");
    const auto filename = program.get<std::string>("filename");
    const auto field = program.get<std::string>("text");
    const std::string empty(n, '_');

    // Open the output file.
    std::ofstream ofs(filename, std::ios::binary);
    if (ofs.fail()) {
        es << "ERROR: failed to open " << filename << std::endl;
        return 1;
    }

    // Write the header: "DoubriH4"
    ofs.write("DoubriH4", 8);
    // Reserve the slot to write the number of items.
    ofs.write(reinterpret_cast<const char*>(&num_items), sizeof(num_items));
    // Write the number of bytes per hash.
    ofs.write(reinterpret_cast<const char*>(&bytes_per_hash), sizeof(bytes_per_hash));
    // Write the number of hash values per bucket.
    ofs.write(reinterpret_cast<const char*>(&num_hash_values), sizeof(num_hash_values));
    // Write the begin index of buckets.
    ofs.write(reinterpret_cast<const char*>(&begin), sizeof(begin));
    // Write the end index of buckets.
    ofs.write(reinterpret_cast<const char*>(&end), sizeof(end));
    // Write a zero for four bytes (reserved).
    ofs.write(reinterpret_cast<const char*>(&zero), sizeof(zero));

    // One JSON object per line.
    for (num_items = 0; ; ++num_items) {
        // Read a line from STDIN.
        std::string line;
        std::getline(is, line);
        if (is.eof()) {
            break;
        }

        // Parse the line in JSON.
        auto d = json::parse(line);

        // Obtain the text.
        std::string text = empty;
        if (d.contains(field)) {
            text = d[field];

            // Make sure that the text is at least n characters.
            if (utf8::distance(text.begin(), text.end()) < n) {
                text = empty;
            }
        }

        // Obtain features (n-grams) from the text.
        std::vector<std::string> features;
        ngram(text, features, n);

        for (int i = begin; i < end; ++i) {
            // Compute min-hash values.
            uint32_t buffer[num_hash_values];
            minhash(features, buffer, i * num_hash_values, num_hash_values);
            // We store MinHash values in big endian so that we can easily
            // check byte streams with a binary editor (for debugging).
            if constexpr (std::endian::native == std::endian::little) {
                // Change the byte order of the hash values to big endian.
                for (int j = 0; j < num_hash_values; ++j) {
                    buffer[j] = std::byteswap(buffer[j]);
                }
            }
            // Write the hash values.
            ofs.write(reinterpret_cast<const char*>(buffer), sizeof(buffer));
        }
    }

    // Write the number of items in the header.
    ofs.seekp(8);
    ofs.write(reinterpret_cast<const char*>(&num_items), sizeof(num_items));
    ofs.close();

    return 0;
}
