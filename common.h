/*
    Dabri common staffs.

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

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

#define __DOUBRI_VERSION__ "2.0"

#define BYTE_PER_HASH 4
#define BUCKET_SIZE 20
#define BYTE_PER_BUCKET (BYTE_PER_HASH * BUCKET_SIZE)
#define NUM_BUCKETS 40
#define BYTE_PER_RECORD (BYTE_PER_BUCKET * NUM_BUCKETS)

typedef std::array<uint8_t, BYTE_PER_BUCKET> bucket_t;

struct kv {
    std::string _key;
    std::string _value;

    kv(std::string_view key, std::string_view value)
        : _key(key), _value("")
    {
        _value = "\"";
        _value += value;
        _value += "\"";
    }
    kv(std::string_view key, size_t value)
        : _key(key), _value("")
    {
        _value = std::to_string(value);
    }
    kv(std::string_view key, double value)
        : _key(key), _value("")
    {
        _value = std::to_string(value);
    }
};

template <typename CharT, typename Traits>
decltype(auto) operator<<(std::basic_ostream<CharT, Traits>& os, kv rhs)
{
    os << '"' << rhs._key << '"' << ": " << rhs._value;
    return os;
}
