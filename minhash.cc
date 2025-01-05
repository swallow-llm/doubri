/*
    MinHash generator.

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

#include <bit>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include <byteswap.h>

#include <utf8.h>
#include <nlohmann/json.hpp>
#include <argparse/argparse.hpp>
#include "MurmurHash3.h"

#include "common.h"
#include "minhash.hpp"

using json = nlohmann::json;

/**
 * Generate n-grams from a UTF-8 string.
 *  This function generates n-grams in UTF-8 character (not in byte).
 *
 *  @param  str     A string.
 *  @param  ngrams  A vector of strings to store n-grams.
 *  @param  n       The number of letters (N).
 */
void ngram(const std::string& str, std::vector<std::string>& ngrams, int n)
{
    std::vector<const char *> cs;
    const char *end = str.c_str() + str.size();

    // Do nothing if the given string is empty.
    if (str.empty()) {
        return;
    }

    // Store pointers to the unicode characters in the given string.
    for (const char *p = str.c_str(); *p; utf8::next(p, end)) {
        cs.push_back(p);
    }
    // Add the pointer to the NULL termination of the string.
    cs.push_back(end);

    // Append n-grams (note that cs.size() is num_letters + 1).
    for (int i = 0; i < cs.size()-n; ++i) {
        const char *b = cs[i];
        const char *e = cs[i+n];
        std::string s(b, e-b);
        ngrams.push_back(s);
    }
}

/**
 * Generate MinHash values for given strings.
 *  This function generates {num} MinHash values (in uint32_t) by using
 *  hash functions #{begin}, #{begin+1}, ..., #{begin+num}, and write
 *  MinHash values to the buffer of {num} * uint32_t bytes.
 *
 *  @param  strs    A target item represented as strings.
 *  @param  out     A pointer to the buffer to receive MinHash values.
 *  @param  begin   A beginning number of hash functions.
 *  @param  num     A number of MinHash values to generate.
 */
void minhash(const std::vector<std::string>& strs, uint32_t *out, size_t begin, size_t num)
{
    for (size_t i = 0; i < num; ++i) {
        const size_t seed = begin + i;
        uint32_t min = 0xFFFFFFFF;
        for (auto it = strs.begin(); it != strs.end(); ++it) {
            uint32_t hv;
            MurmurHash3_x86_32(reinterpret_cast<const void*>(it->c_str()), it->size(), seed, &hv);
            if (hv < min) {
                min = hv;
            }
        }
        out[i] = min;
    }
}

int main(int argc, char *argv[])
{
    size_t num_items = 0;
    std::istream& is = std::cin;
    std::ostream& os = std::cout;
    std::ostream& es = std::cerr;

    // The command-line parser.
    argparse::ArgumentParser program("doubri-minhash", __DOUBRI_VERSION__);
    program.add_description("Read text (in JSONL format) from STDIN and compute MinHash buckets.");
    program.add_argument("-n", "--ngram").metavar("N")
        .help("number of letters of an n-gram")
        .nargs(1)
        .default_value(5)
        .scan<'d', int>();
    program.add_argument("-b", "--bucket").metavar("HASHNUM")
        .help("number of hash values per bucket")
        .nargs(1)
        .default_value(20)
        .scan<'d', int>();
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
    program.add_argument("-t", "--text").metavar("TEXT")
        .help("text field in JSON")
        .nargs(1)
        .default_value("text");
    program.add_argument("-q", "--quiet")
        .help("suppresss messages from the program")
        .flag();
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

    // Retrieve parameters from the command-line arguments.
    const int n = program.get<int>("ngram");
    const int bytes_per_hash = 4;
    const int num_hash_values = program.get<int>("bucket");
    const int begin = program.get<int>("start");
    const int end = program.get<int>("end");
    const bool quiet = program.get<bool>("quiet");
    const auto filename = program.get<std::string>("filename");
    const auto field = program.get<std::string>("text");
    const std::string empty(n, '_');

    // Show the parameters.
    if (!quiet) {
        os << "n: " << n << std::endl;
        os << "bytes_per_hash: " << bytes_per_hash << std::endl;
        os << "num_hash_values: " << num_hash_values << std::endl;
        os << "begin: " << begin << std::endl;
        os << "end: " << end << std::endl;
    }

    // Open the output file.
    MinHashWriter mw;
    mw.open(filename, num_hash_values, begin, end);

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

        // Generate buckets from #{begin} to #{end-1}.
        uint32_t buffer[(end-begin) * num_hash_values];
        for (int i = begin; i < end; ++i) {
            size_t offset = (i-begin) * num_hash_values;
            // Compute min-hash values.
            minhash(features, &buffer[offset], i * num_hash_values, num_hash_values);
        }

        // Put the buckets to the writer.
        mw.put(buffer);
    }

    // Close the writer.
    mw.close();

    // Show the number of items.
    if (!quiet) {
        os << "num_items: " << mw.num_items() << std::endl;
    }

    return 0;
}
