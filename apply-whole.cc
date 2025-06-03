/*
    Filter active items.

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

#include <fstream>
#include <iostream>
#include <string>

#include "common.h"
#include <argparse/argparse.hpp>

int main(int argc, char *argv[])
{
    std::istream& is = std::cin;
    std::ostream& os = std::cout;
    std::ostream& es = std::cerr;

    // The command-line parser.
    argparse::ArgumentParser program("doubri-apply-whole", __DOUBRI_VERSION__);
    program.add_description("Read documents (in JSONL format) from STDIN and output non-duplicate ones to STDOUT.");
    program.add_argument("-f", "--flag").metavar("FLAG")
        .help("specify a flag file marking duplicated documents with 'D'")
        .nargs(1)
        .required();
    program.add_argument("-v", "--verbose")
        .help("output debug information to STDERR (disabled, by default)")
        .default_value(false)
        .flag();

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
    const auto flag_file = program.get<std::string>("flag");
    const auto verbose = program.get<bool>("verbose");

    // Open the flag file.
    std::ifstream iff(flag_file);
    if (iff.fail()) {
        es << "ERROR: Failed to open " << flag_file << std::endl;
        return 1;
    }

    // Check the file size (== total number of items in the source).
    iff.seekg(0, std::ios_base::end);
    if (iff.fail()) {
        es << "ERROR: Failed to seek to the end of the file: " << flag_file << std::endl;
        return 1;
    }
    uint64_t num_total_items = iff.tellg();

    // Read the flags.
    std::vector<char> flags;
    flags.resize(num_total_items);
    iff.seekg(0, std::ios_base::beg);
    if (iff.fail()) {
        es << "ERROR: Failed to seek to the beginning of the file: " << flag_file << std::endl;
        return 1;
    }
    iff.read(reinterpret_cast<std::ifstream::char_type*>(&flags.front()), num_total_items);
    if (iff.fail() || iff.eof()) {
        es << "ERROR: Failed to read " << num_total_items << " bytes in: " << flag_file << std::endl;
        return 1;
    }

    // One JSON object per line.
    uint64_t num_active = 0;
    uint64_t i = 0;
    for (i = 0; i < num_total_items; ++i) {
        // Read a line from STDIN.
        std::string line;
        std::getline(is, line);
        if (is.eof()) {
            es << "ERROR: STDIN hit EOF before " << num_total_items << " lines" << std::endl;
            return 1;
        }

        // Output the line if the flag is true ('1').
        if (flags[i] == ' ') {
            os << line << std::endl;
            ++num_active;
        }
    }

    // Make sure that we now hit EOF from STDIN.
    std::string line;
    std::getline(is, line);
    if (!is.eof()) {
        es << "ERROR: STDIN did hit EOF after " << num_total_items << " lines" << std::endl;
        return 1;
    }

    return 0;
}
