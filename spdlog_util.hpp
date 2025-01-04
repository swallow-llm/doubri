/*
    Common utility for spdlog.

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

#include <stdexcept>
#include <string>
#include <spdlog/spdlog.h>

auto translate_log_level(const std::string& level)
{
    if (level == "off") {
        return spdlog::level::off;
    } else if (level == "trace") {
        return spdlog::level::trace;
    } else if (level == "debug") {
        return spdlog::level::debug;
    } else if (level == "info") {
        return spdlog::level::info;
    } else if (level == "warning") {
        return spdlog::level::warn;
    } else if (level == "error") {
        return spdlog::level::err;
    } else if (level == "critical") {
        return spdlog::level::critical;
    } else {
        std::string msg = std::string("Unknown log level: ") + level;
        throw std::invalid_argument(msg);
    }
}
