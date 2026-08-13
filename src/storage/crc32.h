#pragma once

#include <cstddef>
#include <cstdint>

namespace distdb {

// Standard CRC-32 (IEEE 802.3 polynomial), used to detect corrupt or
// partially-written WAL records after a crash.
uint32_t Crc32(const void* data, size_t len);

}  // namespace distdb
