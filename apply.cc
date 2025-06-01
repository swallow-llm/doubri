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
    argparse::ArgumentParser program("doubri-apply", __DOUBRI_VERSION__);
    program.add_description("Read documents (in JSONL format) from STDIN and output non-duplicate ones to STDOUT.");
    program.add_argument("-f", "--flag").metavar("FLAG")
        .help("specify a flag file marking duplicated documents with 'D'")
        .nargs(1)
        .required();
    program.add_argument("-s", "--source").metavar("SRC")
        .help("specify a file storing the list of source MinHash files for the flag file")
        .nargs(1)
        .required();
    program.add_argument("target").metavar("TARGET")
        .help("specify the MinHash filename corresponding to the input JSONL file (this file does not need to exist)");
    program.add_argument("-d", "--strip")
        .help("strip directory name from source MinHash files (path) when finding the target")
        .default_value(false)
        .flag();
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
    const auto src_file = program.get<std::string>("source");
    const auto target_file = program.get<std::string>("target");
    const auto strip = program.get<bool>("strip");
    const auto verbose = program.get<bool>("verbose");

    // Open the flag file.
    std::ifstream iff(flag_file);
    if (iff.fail()) {
        es << "ERROR: Failed to open " << flag_file << std::endl;
        return 1;
    }

    // Check the file size (== total number of items in the source).
    iff.seekg(0, std::ios_base::end);
    uint64_t num_total_items = iff.tellg();
    iff.seekg(0, std::ios_base::beg);

    // Open the source file.
    std::ifstream ifs(src_file);
    if (ifs.fail()) {
        es << "ERROR: Failed to open " << src_file << std::endl;
        return 1;
    }

    // Read the source file.
    int lines = 0;
    uint64_t n = 0;
    bool found = false;
    uint64_t begin = 0, size = 0;
    for (;;) {
        // Read a line from STDIN.
        std::string line;
        std::getline(ifs, line);
        if (ifs.eof()) {
            break;
        }
        ++lines;

        // Find a TAB separator in "{num_items}\t{source}".
        auto pos = line.find('\t');
        if (pos == -1) {
            es << "ERROR: No TAB separator in lines " << lines << ": " <<
                line << std::endl;
            return 1;
        }

        // Obtain the number of items and source MinHash file.
        uint64_t num_items = std::stoll(std::string(line, 0, pos));
        std::string source(line, pos+1);

        // Strip the directory name from the source path.
        if (strip) {
            const auto p = source.rfind('/');
            if (p != source.npos) {
                source = std::string(source, p+1);
            }
        }

        // Record the range for the target.
        if (source == target_file) {
            if (!found) {
                begin = n;
                size = num_items;
                found = true;
            } else {
                es << "ERROR: Possibly a duplicated source at lines " <<
                    lines << ": " << source << std::endl;
                return 1;
            }
        }

        n += num_items;
    }

    // Make sure that numbers of items in the flag and source are the same.
    if (num_total_items != n) {
        es << "ERROR: Inconsistent numbers of items: " <<
            num_total_items << " (from flag), " <<
            n << " (from source)" << std::endl;
        return 1;
    }

    // Exit if the target does not exist in the source.
    if (!found) {
        es << "ERROR: The target does not exist in the source: " <<
            target_file << std::endl;
        return 1;
    }

    // Read the flags for the target.
    std::vector<char> flags;
    flags.resize(size);
    iff.seekg(begin, std::ios_base::beg);
    if (iff.fail()) {
        es << "ERROR: Failed to seek to " << begin << " in: " << flag_file << std::endl;
        return 1;
    }
    iff.read(reinterpret_cast<std::ifstream::char_type*>(&flags.front()), size);
    if (iff.fail() || iff.eof()) {
        es << "ERROR: Failed to read " << size << " bytes in: " << flag_file << std::endl;
        return 1;
    }

    // One JSON object per line.
    uint64_t num_active = 0;
    uint64_t i = 0;
    for (;; ++i) {
        // Read a line from STDIN.
        std::string line;
        std::getline(is, line);
        if (is.eof()) {
            break;
        }

        if (size <= i) {
            es << "ERROR: STDIN is longer than " << size << " lines" << std::endl;
            return 1;
        }

        // Output the line if the flag is true ('1').
        if (flags[i] == ' ') {
            os << line << std::endl;
            ++num_active;
        }
    }

    // Output debug information.
    if (verbose) {
        es << "flag: " << flag_file << std::endl;
        es << "source: " << src_file << std::endl;
        es << "target: " << target_file << std::endl;
        es << "begin: " << begin << std::endl;
        es << "size: " << size << std::endl;
        es << "num_active: " << num_active << std::endl;
        for (auto it = flags.begin(); it != flags.end(); ++it) {
            es << *it;
        }
        es << std::endl;
    }
    
    if (i < size) {
        es << "ERROR: STDIN is shorter than " << size << " lines" << std::endl;
        return 1;
    }

    return 0;
}
