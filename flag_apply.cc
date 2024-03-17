/*
    Filter active items.

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

#include <fstream>
#include <iostream>
#include <string>

int main(int argc, char *argv[])
{
    size_t num = 0;
    std::istream& is = std::cin;
    std::ostream& os = std::cout;
    std::ostream& es = std::cerr;

    // Open the flag file.
    std::ifstream ifs(argv[1]);
    if (ifs.fail()) {
        es << "ERROR: Failed to open " << argv[1] << std::endl;
        return 1;
    }
    
    // One JSON object per line.
    for (;;) {
        char c = 0;

        // Read a line from STDIN.
        std::string line;
        std::getline(is, line);
        if (is.eof()) {
            // Make sure that the flag file also hits EOF at the same time.
            ifs >> c;
            if (!ifs.eof()) {
                es << "ERROR: Premature end of the input stream." << std::endl;
                return 1;
            }
            break;
        }

        // Read a flag.
        ifs >> c;
        if (ifs.eof()) {
            es << "ERROR: Premature end of the flag file." << std::endl;
            return 1;
        }

        // Output the line if the flag is true ('1').
        if (c == '1') {
            os << line << std::endl;
        }
    }

    return 0;
}
