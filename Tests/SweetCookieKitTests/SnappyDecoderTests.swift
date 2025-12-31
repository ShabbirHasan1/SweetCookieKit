import Foundation
import Testing
@testable import SweetCookieKit

#if os(macOS)

struct SnappyDecoderTests {
    @Test
    func decompressesLiteralBlock() {
        let payload = Array("hello".utf8)
        var data = Data()
        data.append(self.varint32(payload.count))
        data.append(UInt8((payload.count - 1) << 2))
        data.append(contentsOf: payload)

        let decoded = SnappyDecoder.decompress(data)

        #expect(decoded == Data(payload))
    }

    @Test
    func decompressesCopyType1() {
        var data = Data()
        data.append(self.varint32(9))
        data.append(UInt8((3 - 1) << 2))
        data.append(contentsOf: Array("abc".utf8))
        data.append(UInt8(((6 - 4) << 2) | 0x01))
        data.append(0x03)

        let decoded = SnappyDecoder.decompress(data)

        #expect(decoded == Data("abcabcabc".utf8))
    }

    @Test
    func decompressesCopyType2() {
        var data = Data()
        data.append(self.varint32(8))
        data.append(UInt8((4 - 1) << 2))
        data.append(contentsOf: Array("abcd".utf8))
        data.append(UInt8(((4 - 1) << 2) | 0x02))
        data.append(contentsOf: [0x04, 0x00])

        let decoded = SnappyDecoder.decompress(data)

        #expect(decoded == Data("abcdabcd".utf8))
    }

    @Test
    func decompressesCopyType3() {
        var data = Data()
        data.append(self.varint32(10))
        data.append(UInt8((5 - 1) << 2))
        data.append(contentsOf: Array("hello".utf8))
        data.append(UInt8(((5 - 1) << 2) | 0x03))
        data.append(contentsOf: [0x05, 0x00, 0x00, 0x00])

        let decoded = SnappyDecoder.decompress(data)

        #expect(decoded == Data("hellohello".utf8))
    }

    @Test
    func decompressesLongLiteral() {
        let payload = Array(repeating: UInt8(ascii: "a"), count: 70)
        var data = Data()
        data.append(self.varint32(payload.count))
        data.append(contentsOf: self.snappyLiteralTagBytes(length: payload.count))
        data.append(contentsOf: payload)

        let decoded = SnappyDecoder.decompress(data)

        #expect(decoded == Data(payload))
    }

    @Test
    func returnsNilForTruncatedLiteral() {
        let payload = Array("hello".utf8)
        var data = Data()
        data.append(self.varint32(payload.count))
        data.append(UInt8((payload.count - 1) << 2))
        data.append(contentsOf: payload.dropLast())

        let decoded = SnappyDecoder.decompress(data)

        #expect(decoded == nil)
    }
}

extension SnappyDecoderTests {
    private func varint32(_ value: Int) -> Data {
        var result = Data()
        var remaining = UInt32(value)
        while true {
            if remaining & ~0x7F == 0 {
                result.append(UInt8(remaining))
                break
            }
            result.append(UInt8((remaining & 0x7F) | 0x80))
            remaining >>= 7
        }
        return result
    }

    private func snappyLiteralTagBytes(length: Int) -> [UInt8] {
        guard length > 0 else { return [0] }
        if length < 60 {
            return [UInt8((length - 1) << 2)]
        }
        var remaining = length - 1
        var bytes: [UInt8] = []
        while remaining > 0 {
            bytes.append(UInt8(remaining & 0xFF))
            remaining >>= 8
        }
        let tag = UInt8((59 + bytes.count) << 2)
        return [tag] + bytes
    }
}

#endif
