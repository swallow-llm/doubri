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
    uint64_t num_records = 0;
    std::istream& is = std::cin;
    std::ostream& os = std::cout;
    std::ostream& es = std::cerr;
    argparse::ArgumentParser program("doubri-minhash", __DOUBRI_VERSION__);

    program.add_argument("-n", "--ngram").metavar("N")
        .help("the number of letters of an n-gram")
        .default_value(5)
        .nargs(1);
    program.add_argument("-b", "--bucket").metavar("HASHNUM")
        .help("the number of hash values per bucket")
        .default_value(20)
        .nargs(1);
    program.add_argument("-s", "--start").metavar("START")
        .help("the start number of buckets")
        .default_value(0)
        .nargs(1);
    program.add_argument("-r", "--end").metavar("END")
        .help("the end number of buckets (the number of buckets when START = 0)")
        .default_value(40)
        .nargs(1);
    program.add_argument("-t", "--text").metavar("TEXT")
        .help("the text field in JSON")
        .default_value("text")
        .nargs(1);
    program.add_argument("filename").metavar("FILENAME")
        .help("a prefix of filenames, {FILENAME}.mh.{START}, ... {FILENAME}.mh.{END-1}");

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
    const uint32_t num_hash_values = (uint32_t)program.get<int>("bucket");
    const auto begin = program.get<int>("start");
    const auto end = program.get<int>("end");
    const auto prefix = program.get<std::string>("filename");
    const auto field = program.get<std::string>("text");
    const std::string empty(n, '_');

    // Open the output file.
    std::vector<std::ofstream> ofss;
    for (int i = begin; i < end; ++i) {
        // Obtain the filename for the bucket #i.
        std::stringstream ss;
        ss << prefix << ".mh." << std::setfill('0') << std::setw(5) << i;
        std::string filename = ss.str();

        // Open the filename for the bucket #i.
        ofss.emplace_back(std::ofstream{ filename, std::ios::binary });

        // Error check.
        if (ofss.back().fail()) {
            es << "ERROR: failed to open " << filename << std::endl;
            return 1;
        }
    }

    for (int i = begin; i < end; ++i) {
        std::ofstream& ofs = ofss[i-begin];

        // Write the header: "Doubri20"
        ofs << "Doubri20";        
        // Reserve the slot to write the number of records.
        ofs.write(reinterpret_cast<const char*>(&num_records), sizeof(num_records));
        // Write the number of hash values per bucket.
        ofs.write(reinterpret_cast<const char*>(&num_hash_values), sizeof(num_hash_values));
    }

    // One JSON object per line.
    for (num_records = 0; ; ++num_records) {
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
            // Write the hash values.
            ofss[i-begin].write(reinterpret_cast<const char*>(buffer), sizeof(buffer));
        }
    }

    // Write the number of records in the header.
    for (int i = begin; i < end; ++i) {
        std::ofstream& ofs = ofss[i-begin];

        ofs.seekp(8);
        ofs.write(reinterpret_cast<const char*>(&num_records), sizeof(num_records));
        ofs.close();
    }

    return 0;
}
