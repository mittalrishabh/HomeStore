#pragma once
#include <cstdint>
#include <cstddef>

#if defined(__aarch64__) || defined(_M_ARM64)
#include <arm_acle.h>

__attribute__((target("arch=armv8-a+crc")))
static inline uint32_t bt_crc32c(uint32_t seed, const unsigned char* buf, uint64_t len) {
    uint32_t crc = seed;

    // Process 8 bytes at a time using ARM CRC32C hardware instruction
    while (len >= 8) {
        uint64_t val;
        __builtin_memcpy(&val, buf, 8);
        crc = __crc32cd(crc, val);
        buf += 8;
        len -= 8;
    }

    // Process 4 bytes
    if (len >= 4) {
        uint32_t val;
        __builtin_memcpy(&val, buf, 4);
        crc = __crc32cw(crc, val);
        buf += 4;
        len -= 4;
    }

    // Process 2 bytes
    if (len >= 2) {
        uint16_t val;
        __builtin_memcpy(&val, buf, 2);
        crc = __crc32ch(crc, val);
        buf += 2;
        len -= 2;
    }

    // Process remaining byte
    if (len) {
        crc = __crc32cb(crc, *buf);
    }

    return crc;
}

#elif defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)

#ifndef NO_ISAL
#include <isa-l/crc.h>
static inline uint32_t bt_crc32c(uint32_t seed, const unsigned char* buf, uint64_t len) {
    return crc32_iscsi(buf, len, seed);
}
#else
#include <nmmintrin.h> // SSE4.2

__attribute__((target("sse4.2")))
static inline uint32_t bt_crc32c(uint32_t seed, const unsigned char* buf, uint64_t len) {
    uint32_t crc = seed;

    // Process 8 bytes at a time
    while (len >= 8) {
        uint64_t val;
        __builtin_memcpy(&val, buf, 8);
        crc = (uint32_t)_mm_crc32_u64(crc, val);
        buf += 8;
        len -= 8;
    }

    if (len >= 4) {
        uint32_t val;
        __builtin_memcpy(&val, buf, 4);
        crc = _mm_crc32_u32(crc, val);
        buf += 4;
        len -= 4;
    }

    while (len--) {
        crc = _mm_crc32_u8(crc, *buf++);
    }

    return crc;
}
#endif

#else
// Generic fallback — software CRC32C
static inline uint32_t bt_crc32c(uint32_t seed, const unsigned char* buf, uint64_t len) {
    // CRC32C polynomial: 0x82F63B78
    static const uint32_t table[256] = {
        0x00000000, 0xF26B8303, 0xE13B70F7, 0x1350F3F4, 0xC79A971F, 0x35F1141C, 0x26A1E7E8, 0xD4CA64EB,
        0x8AD958CF, 0x78B2DBCC, 0x6BE22838, 0x9989AB3B, 0x4D43CFD0, 0xBF284CD3, 0xAC78BF27, 0x5E133C24,
        0x105EC76F, 0xE235446C, 0xF165B798, 0x030E349B, 0xD7C45070, 0x25AFD373, 0x36FF2087, 0xC494A384,
        0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57, 0x89D76C54, 0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B,
        0x20BD8EDE, 0xD2D60DDD, 0xC186FE29, 0x33ED7D2A, 0xE72719C1, 0x15AC9AC2, 0x06FC6936, 0xF497EA35,
        0xAA84D611, 0x58EF5512, 0x4BBFA6E6, 0xB9D425E5, 0x6D1E410E, 0x9F75C20D, 0x8C2531F9, 0x7E4EB2FA,
        0x30036961, 0xC268EA62, 0xD1381996, 0x23539A95, 0xF799FE7E, 0x05F27D7D, 0x16A28E89, 0xE4C90D8A,
        0xBADA31AE, 0x48B1B2AD, 0x5BE14159, 0xA98AC25A, 0x7D40A6B1, 0x8F2B25B2, 0x9C7BD646, 0x6E105545,
        0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x5228EE48, 0x86E28AA3, 0x74890DA0, 0x67D9FE54, 0x95B27D57,
        0xCBA14173, 0x39CAC270, 0x2A9A3184, 0xD8F1B287, 0x0C3BD66C, 0xFE50556F, 0xED00A69B, 0x1F6B2598,
        0x5125FE03, 0xA34E7D00, 0xB01E8EF4, 0x42750DF7, 0x96BF691C, 0x64D4EA1F, 0x77841963, 0x85EF9A60,
        0xDBFCA644, 0x29972547, 0x3AC7D6B3, 0xC8AC55B0, 0x1C66315B, 0xEE0DB258, 0xFD5D41AC, 0x0F36C2AF,
        0x6FC6B73A, 0x9DAD3439, 0x8EFDC7CD, 0x7C9644CE, 0xA85C2025, 0x5A37A326, 0x496750D2, 0xBB0CD3D1,
        0xE51FEFF5, 0x177471F6, 0x04248202, 0xF64F0101, 0x228565EA, 0xD0EEE6E9, 0xC3BE151D, 0x31D5961E,
        0x7F986B85, 0x8DF3E886, 0x9EA31B72, 0x6CC89871, 0xB802FC9A, 0x4A697F99, 0x59398C6D, 0xAB520F6E,
        0xF541334A, 0x072AB049, 0x147A43BD, 0xE611C0BE, 0x32DBA455, 0xC0B02756, 0xD3E0D4A2, 0x218B57A1,
        0x82F63B78, 0x709DB87B, 0x63CD4B8F, 0x91A6C88C, 0x456CAC67, 0xB7072F64, 0xA457DC90, 0x563C5F93,
        0x082F63B7, 0xFA44E0B4, 0xE9141340, 0x1B7F9043, 0xCFB5F4A8, 0x3DDE77AB, 0x2E8E845F, 0xDCE5075C,
        0x92A8FC47, 0x60C37F44, 0x73938CB0, 0x81F80FB3, 0x55326B58, 0xA759E85B, 0xB4091BAF, 0x46628AAC,
        0x1871B688, 0xEA1A358B, 0xF94AC67F, 0x0B21457C, 0xDFEB2197, 0x2D80A294, 0x3ED05160, 0xCCBBD263,
        0xAC4BA7F8, 0x5E20D4FB, 0x4D70270F, 0xBF1BA40C, 0x6BD1C0E7, 0x99BA43E4, 0x8AEAB010, 0x78813313,
        0x26920F37, 0xD4F98C34, 0xC7A97FC0, 0x35C2FCC3, 0xE1089828, 0x13631B2B, 0x0033E8DF, 0xF2586BDC,
        0xBC15B047, 0x4E7E3344, 0x5D2EC0B0, 0xAF4543B3, 0x7B8F2758, 0x89E4A45B, 0x9AB457AF, 0x68DFD4AC,
        0x36CCE888, 0xC4A76B8B, 0xD7F7987F, 0x25901B7C, 0xF15A7F97, 0x0331FC94, 0x10610F60, 0xE20A8C63,
        0xE6EF15DB, 0x1484D6D8, 0x07D4252C, 0xF5BFA62F, 0x2175C2C4, 0xD31E41C7, 0xC04EB233, 0x32253130,
        0x6C360D14, 0x9E5D8E17, 0x8D0D7DE3, 0x7F66FEE0, 0xABAC9A0B, 0x59C71908, 0x4A97EAFC, 0xB8FC69FF,
        0xF6B19264, 0x04DA1167, 0x178AE293, 0xE5E16190, 0x312B057B, 0xC3408678, 0xD010758C, 0x227BF68F,
        0x7C68CAAB, 0x8E0349A8, 0x9D53BA5C, 0x6F38395F, 0xBBF25DB4, 0x4999DEB7, 0x5AC92D43, 0xA8A2AE40,
        0xC8527BD5, 0x3A39F8D6, 0x29690B22, 0xDB028821, 0x0FC8ECFA, 0xFDA36FF9, 0xEEF39C0D, 0x1C981F0E,
        0x428B232A, 0xB0E0A029, 0xA3B053DD, 0x51DBD0DE, 0x856BB435, 0x77003736, 0x6450C4C2, 0x966B47C1,
        0xD826BC5A, 0x2A4D3F59, 0x391DCCAD, 0xCB764FAE, 0x1FBC2B45, 0xEDD7A846, 0xFE875BB2, 0x0CECD8B1,
        0x52FFE495, 0xA0946796, 0xB3C49462, 0x41AF1761, 0x9565738A, 0x670EF089, 0x745E037D, 0x8635807E,
    };

    uint32_t crc = seed ^ 0xFFFFFFFF;
    while (len--) {
        crc = table[(crc ^ *buf++) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}
#endif
