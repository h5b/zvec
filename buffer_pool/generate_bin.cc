#include <stdio.h>
#include <string>
#include <algorithm>
#include <iostream>

int main(int argc, char** argv) {
    std::string output_path = argv[1];
    int size_in_gb = atoi(argv[2]);

    size_t total_size = static_cast<size_t>(size_in_gb) * 1024 * 1024 * 1024;
    FILE* fout = fopen(output_path.c_str(), "wb");
    if (!fout) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return -1;
    }
    const size_t buffer_size = 16 * 1024 * 1024;  // 16 MB
    char* buffer = new char[buffer_size];
    for (size_t i = 0; i < buffer_size; i++) {
        buffer[i] = static_cast<char>(i % 256);
    }
    size_t written = 0;
    while (written < total_size) {
        size_t to_write = std::min(buffer_size, total_size - written);
        size_t written_bytes = fwrite(buffer, 1, to_write, fout);
        if (written_bytes != to_write) {
            std::cerr << "Failed to write to output file." << std::endl;
            delete[] buffer;
            fclose(fout);
            return -1;
        }
        written += written_bytes;
    }
    delete[] buffer;
    fclose(fout);
    return 0;
}