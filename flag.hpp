/*
    Reader and writer for flag files (.dup).

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

#pragma once

#include <fstream>
#include <string>

std::string flag_load(const std::string& filename, std::vector<char>& flags)
{
    // Open the flag file.
    std::ifstream ifs(filename);
    if (ifs.fail()) {
        return std::string("Failed to open a flag file: ") + filename;
    }

    // Obtain the file size.
    ifs.seekg(0, std::ios::end);
    auto size = ifs.tellg();
    ifs.seekg(0, std::ios::beg);
    if (ifs.fail()) {
        return std::string("Failed to obtain the file size: ") + filename;
    }

    // Resize the flag vector.
    flags.resize(size);

    // Read the flags at a time.
    ifs.read(reinterpret_cast<std::ifstream::char_type*>(&flags.front()), size);

    // Error check.
    if (ifs.eof()) {
        return std::string("Premature EOF of the file: ") + filename;
    }
    if (ifs.fail()) {
        return std::string("Failed to read flags from the file: ") + filename;
    }

    // Exit with an empty error message.
    return std::string("");
}

std::string flag_save(const std::string& filename, const std::vector<char>& flags)
{
    // Open the flag file.
    std::ofstream ofs(filename);
    if (ofs.fail()) {
        return std::string("Failed to open a flag file: ") + filename;
    }

    // Write the flags.
    ofs.write(reinterpret_cast<const std::ifstream::char_type*>(&flags.front()), flags.size());

    // Error check.
    if (ofs.fail()) {
        return std::string("Failed to write flags to the file: ") + filename;
    }

    // Exit with an empty error message.
    return std::string("");
}
