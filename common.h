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

#include <cstdint>
#include <string>
#include <stdexcept>
#include <sstream>
#include <byteswap.h>

#define __DOUBRI_VERSION__ "2.0"

template <typename StreamT, typename ValueT>
inline void write_value(std::ostream& os, ValueT value)
{
    StreamT value_ = static_cast<StreamT>(value);
    if (static_cast<ValueT>(value_) != value) {
        std::stringstream ss;
        ss << "Impossible to store " << value << " in " << sizeof(StreamT) << " bytes";
        throw std::range_error(ss.str());
    }
    os.write(reinterpret_cast<char*>(&value_), sizeof(value_));
}

template <typename StreamT, typename ValueT>
inline ValueT read_value(std::istream& is)
{
    StreamT value_;
    is.read(reinterpret_cast<char*>(&value_), sizeof(value_));
    return static_cast<ValueT>(value_);
}

