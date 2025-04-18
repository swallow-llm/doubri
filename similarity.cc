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

#define USE_XXHASH

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <utf8.h>
#include <nlohmann/json.hpp>
#include <argparse/argparse.hpp>
#include <tbb/parallel_for_each.h>
#include <tbb/mutex.h>

#include "common.h"

using json = nlohmann::json;

struct Item {
    std::string _id;
    std::string _text;
    std::unordered_set<std::string> _ngrams;
    Item(const std::string& id, const std::string& text) : _id(id), _text(text)
    {
    }
};

/**
 * Generate n-grams from a UTF-8 string.
 *  This function generates n-grams from the string \c str in UTF-8 character
 *  (not in byte) and insert them to the set container \c ngrams. If the
 *  length of the string is shorter than \c n, this function does not change
 *  the container \c ngrams.
 *
 *  @param  str     A string.
 *  @param  ngrams  An unordered set of strings to store n-grams.
 *  @param  n       The number of letters of n-grams (N).
 */
void ngram(const std::string& str, std::unordered_set<std::string>& ngrams, int n)
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
    for (int i = 0; i < (int)cs.size()-n; ++i) {
        const char *b = cs[i];
        const char *e = cs[i+n];
        std::string s(b, e-b);
        ngrams.insert(s);
    }
}

int main(int argc, char *argv[])
{
    std::istream& is = std::cin;
    std::ostream& os = std::cout;
    std::ostream& es = std::cerr;
    tbb::mutex mutex;

    // The command-line parser.
    argparse::ArgumentParser program("doubri-similarity", __DOUBRI_VERSION__);
    program.add_description("Read text (in JSONL format) from STDIN and compute similarity of all text.");
    program.add_argument("-n", "--ngram").metavar("N")
        .help("number of letters of an n-gram")
        .nargs(1)
        .default_value(5)
        .scan<'d', int>();
    program.add_argument("-s", "--threshold").metavar("THRESHOLD")
        .help("minimum similarity")
        .nargs(1)
        .default_value(0.6)
        .scan<'f', double>();
    program.add_argument("-i", "--id").metavar("ID")
        .help("id field in JSON")
        .nargs(1)
        .default_value("id");
    program.add_argument("-t", "--text").metavar("TEXT")
        .help("text field in JSON")
        .nargs(1)
        .default_value("text");

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
    const auto field_id = program.get<std::string>("id");
    const auto field_text = program.get<std::string>("text");
    const auto threshold = program.get<double>("threshold");

    // One JSON object per line.
    std::vector<Item> items;
    for (;;) {
        // Read a line from STDIN.
        std::string line;
        std::getline(is, line);
        if (is.eof()) {
            break;
        }

        // Parse the line in JSON.
        auto d = json::parse(line);

        // Obtain the text.
        if (d.contains(field_id) && d.contains(field_text)) {
            std::string text = d[field_text];
            if (utf8::distance(text.begin(), text.end()) < n) {
                text = std::string(n, '_');
            }
            Item item(d[field_id], text);
            ngram(item._text, item._ngrams, n);
            items.push_back(item);
        }
    }

    tbb::parallel_for((size_t)0, (size_t)items.size(), [&](size_t i) {
        for (size_t j = i+1; j < items.size(); ++j) {
            const auto x = items[i];
            const auto y = items[j];

            size_t m = 0;
            for (auto it = x._ngrams.begin(); it != x._ngrams.end(); ++it) {
                if (y._ngrams.find(*it) != y._ngrams.end()) {
                    ++m;
                }
            }
            size_t n = x._ngrams.size() + y._ngrams.size() - m;
            double sim = m / (double)n;
            if (threshold <= sim) {
                tbb::mutex::scoped_lock lock(mutex);
                os << sim << " " << x._id << " " << y._id << std::endl;
            }
        }
    });

    return 0;
}
