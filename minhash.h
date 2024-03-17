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

#pragma once

#include <cstring>
#include <string>
#include <vector>
#include <utf8.h>
#include "MurmurHash3.h"

void ngram(const std::string& str, std::vector<std::string>& fs, int n)
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
        fs.push_back(s);
    }
}

size_t minhash(const std::vector<std::string> input, uint32_t *output, size_t num)
{
    for (size_t i = 0; i < num; ++i) {
        uint32_t min = 0xFFFFFFFF;
        for (auto it = input.begin(); it != input.end(); ++it) {
            uint32_t hv;
            MurmurHash3_x86_32(reinterpret_cast<const void*>(it->c_str()), it->size(), i, &hv);
            if (hv < min) {
                min = hv;
            }
        }
        output[i] = min;
    }
    return sizeof(uint32_t) * num;
}
