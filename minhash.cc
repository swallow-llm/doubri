/*
    MinHash calculator.

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
#include "minhash.h"

using json = nlohmann::json;

int main(int argc, char *argv[])
{
    size_t n = 5;
    uint64_t num_records = 0;
    const uint64_t num_hash_values = 800; // (b, r) = (20, 40)
    const uint8_t byte_per_hash = (int8_t)4; // 32 bit.
    std::istream& is = std::cin;
    std::ostream& os = std::cout;
    std::ostream& es = std::cerr;

    // Open the output file.
    std::ofstream ofs(argv[1], std::ios::binary);
    if (ofs.fail()) {
        es << "ERROR: failed to open " << argv[1] << std::endl;
        return 1;
    }

    // Write the header: "MinHash"4
    ofs << "MinHash";
    ofs.write(reinterpret_cast<const char*>(&byte_per_hash), sizeof(byte_per_hash));
    
    // Reserve the slot to write the number of records.
    auto pos = ofs.tellp();
    ofs.write(reinterpret_cast<const char*>(&num_records), sizeof(num_records));
    
    // Write the number of hash values per record.
    ofs.write(reinterpret_cast<const char*>(&num_hash_values), sizeof(num_hash_values));

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
        if (d.contains("text")) {
            std::string text = d["text"];

	    // Make sure that the text is at least n characters.
	    if (utf8::distance(text.begin(), text.end()) < n) {
	      text = "EMPTY";
	    }

            // Obtain features (n-grams) from the text.
            std::vector<std::string> features;
            ngram(text, features, n);

            // Compute min-hash values.
            uint32_t buffer[num_hash_values];
            minhash(features, buffer, num_hash_values);

            // Write the hash values.
            ofs.write(reinterpret_cast<const char*>(buffer), sizeof(buffer));

#if 0
            for (uint64_t i = 0; i < num_hash_values; ++i) {
                os << std::dec << "hash[" << num_records << "][" << i << "]: ";
                for (uint8_t j = 0; j < byte_per_hash; ++j) {
                    os << std::setfill('0') << std::setw(2) << std::hex << (unsigned int)reinterpret_cast<unsigned char*>(&buffer)[i*byte_per_hash+j];
                }
                os << std::endl;
            }
#endif
        } else {
            es << "ERROR: the line does not include 'text' key." << std::endl;
            return 1;
        }
    }

    // Write the number of records in the header.
    ofs.seekp(pos);
    ofs.write(reinterpret_cast<const char*>(&num_records), sizeof(num_records));
    ofs.close();

    return 0;
}
