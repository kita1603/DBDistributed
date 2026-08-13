#include "crc32.h"

namespace distdb {

namespace {

uint32_t ComputeTableEntry(uint32_t n) {
    uint32_t c = n;
    for (int k = 0; k < 8; k++) {
        c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
    }
    return c;
}

struct CrcTable {
    uint32_t entries[256];
    CrcTable() {
        for (uint32_t n = 0; n < 256; n++) entries[n] = ComputeTableEntry(n);
    }
};

const CrcTable kCrcTable;

}  // namespace

uint32_t Crc32(const void* data, size_t len) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) {
        crc = kCrcTable.entries[(crc ^ bytes[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

}  // namespace distdb
