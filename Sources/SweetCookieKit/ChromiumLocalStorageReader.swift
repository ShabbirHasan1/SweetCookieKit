import Foundation

#if os(macOS)

public struct ChromiumLocalStorageEntry: Sendable {
    public let origin: String
    public let key: String
    public let value: String
    public let rawValueLength: Int

    public init(origin: String, key: String, value: String, rawValueLength: Int) {
        self.origin = origin
        self.key = key
        self.value = value
        self.rawValueLength = rawValueLength
    }
}

public struct ChromiumLevelDBTextEntry: Sendable {
    public let key: String
    public let value: String

    public init(key: String, value: String) {
        self.key = key
        self.value = value
    }
}

public enum ChromiumLocalStorageReader {
    private static let blockSize = 32 * 1024
    private static let footerSize = 48
    private static let tokenBytes = Set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-+/=".utf8)

    public static func readEntries(
        for origin: String,
        in levelDBURL: URL,
        logger: ((String) -> Void)? = nil) -> [ChromiumLocalStorageEntry]
    {
        let normalizedOrigin = self.normalizeOrigin(origin)
        let log: (String) -> Void = { message in
            logger?("[chromium-storage] \(message)")
        }

        guard let entries = self.levelDBEntries(in: levelDBURL, logger: log) else {
            return []
        }

        var values: [String: String] = [:]
        var rawLengths: [String: Int] = [:]
        var tombstones = Set<String>()
        var decodedKeys = 0
        for entry in entries {
            guard let localKey = self.decodeLocalStorageKey(entry.key) else { continue }
            decodedKeys += 1
            let entryOrigin = self.normalizeOrigin(localKey.origin)
            guard self.originMatches(entryOrigin, normalizedOrigin) else { continue }
            let storageKey = localKey.key

            if entry.isDeletion {
                tombstones.insert(storageKey)
                values.removeValue(forKey: storageKey)
                continue
            }

            guard !tombstones.contains(storageKey) else { continue }
            guard values[storageKey] == nil else { continue }
            guard let decoded = self.decodeLocalStorageValue(entry.value) else { continue }
            values[storageKey] = decoded
            rawLengths[storageKey] = entry.value.count
        }

        if decodedKeys == 0 {
            log("No local storage keys decoded in \(levelDBURL.lastPathComponent)")
        } else if values.isEmpty {
            log("No local storage values for origin \(normalizedOrigin)")
        } else {
            log("Local storage values for origin \(normalizedOrigin): \(values.count)")
        }

        return values.map {
            ChromiumLocalStorageEntry(
                origin: normalizedOrigin,
                key: $0.key,
                value: $0.value,
                rawValueLength: rawLengths[$0.key] ?? $0.value.utf8.count)
        }
    }

    public static func readTextEntries(
        in levelDBURL: URL,
        logger: ((String) -> Void)? = nil) -> [ChromiumLevelDBTextEntry]
    {
        let log: (String) -> Void = { message in
            logger?("[chromium-storage] \(message)")
        }

        guard let entries = self.levelDBEntries(in: levelDBURL, logger: log) else {
            return []
        }

        var results: [ChromiumLevelDBTextEntry] = []
        results.reserveCapacity(entries.count)
        for entry in entries {
            guard let key = self.decodeText(entry.key) else { continue }
            let decoded = self.decodeText(entry.value)
            let stripped = self.decodeLocalStorageValue(entry.value)
            let value = self.pickBestValue(decoded, stripped)
            guard let value else { continue }
            results.append(ChromiumLevelDBTextEntry(key: key, value: value))
        }

        return results
    }

    public static func readTokenCandidates(
        in levelDBURL: URL,
        minimumLength: Int = 60,
        logger: ((String) -> Void)? = nil) -> [String]
    {
        let log: (String) -> Void = { message in
            logger?("[chromium-storage] \(message)")
        }

        guard let entries = self.levelDBEntries(in: levelDBURL, logger: log) else {
            return []
        }

        var tokens = Set<String>()
        for entry in entries {
            tokens.formUnion(self.scanTokens(in: entry.key, minimumLength: minimumLength))
            tokens.formUnion(self.scanTokens(in: entry.value, minimumLength: minimumLength))
        }
        return Array(tokens)
    }

    // MARK: - LevelDB traversal

    private struct LevelDBEntry: Sendable {
        let key: Data
        let value: Data
        let isDeletion: Bool
    }

    private static func levelDBEntries(
        in levelDBURL: URL,
        logger: ((String) -> Void)? = nil) -> [LevelDBEntry]?
    {
        guard let entries = try? FileManager.default.contentsOfDirectory(
            at: levelDBURL,
            includingPropertiesForKeys: [.contentModificationDateKey],
            options: [.skipsHiddenFiles])
        else { return nil }

        let files = entries.filter { url in
            let ext = url.pathExtension.lowercased()
            return ext == "ldb" || ext == "log"
        }
        .sorted { lhs, rhs in
            let left = (try? lhs.resourceValues(forKeys: [.contentModificationDateKey]).contentModificationDate)
            let right = (try? rhs.resourceValues(forKeys: [.contentModificationDateKey]).contentModificationDate)
            return (left ?? .distantPast) > (right ?? .distantPast)
        }

        var results: [LevelDBEntry] = []
        for file in files {
            let ext = file.pathExtension.lowercased()
            if ext == "log" {
                let logEntries = self.readLogEntries(from: file, logger: logger)
                if logEntries.isEmpty {
                    logger?("LevelDB log yielded no entries for \(file.lastPathComponent)")
                }
                results.append(contentsOf: logEntries)
            } else {
                let tableEntries = self.readTableEntries(from: file, logger: logger)
                if tableEntries.isEmpty {
                    logger?("LevelDB table yielded no entries for \(file.lastPathComponent)")
                }
                results.append(contentsOf: tableEntries)
            }
        }
        return results
    }

    // MARK: - Log parsing

    private enum LogRecordType: UInt8 {
        case full = 1
        case first = 2
        case middle = 3
        case last = 4
    }

    private static func readLogEntries(from url: URL, logger: ((String) -> Void)? = nil) -> [LevelDBEntry] {
        guard let data = try? Data(contentsOf: url, options: [.mappedIfSafe]) else { return [] }
        var entries: [LevelDBEntry] = []
        var recordBuffer = Data()
        var offset = 0

        while offset < data.count {
            let blockEnd = min(offset + self.blockSize, data.count)
            var blockOffset = offset
            while blockOffset + 7 <= blockEnd {
                let length = Int(self.readUInt16LE(data, at: blockOffset + 4))
                let type = data[blockOffset + 6]
                blockOffset += 7
                if length == 0 { continue }
                guard blockOffset + length <= blockEnd else { break }
                let chunk = data.subdata(in: blockOffset..<(blockOffset + length))
                blockOffset += length

                guard let recordType = LogRecordType(rawValue: type) else { continue }
                switch recordType {
                case .full:
                    entries.append(contentsOf: self.decodeWriteBatch(chunk))
                case .first:
                    recordBuffer = chunk
                case .middle:
                    recordBuffer.append(chunk)
                case .last:
                    recordBuffer.append(chunk)
                    entries.append(contentsOf: self.decodeWriteBatch(recordBuffer))
                    recordBuffer.removeAll(keepingCapacity: true)
                }
            }
            offset += self.blockSize
        }
        if !recordBuffer.isEmpty {
            entries.append(contentsOf: self.decodeWriteBatch(recordBuffer))
        }
        return Array(entries.reversed())
    }

    private static func decodeWriteBatch(_ data: Data) -> [LevelDBEntry] {
        guard data.count >= 12 else { return [] }
        var entries: [LevelDBEntry] = []
        var offset = 12
        while offset < data.count {
            guard let tag = self.readUInt8(data, at: &offset) else { break }
            switch tag {
            case 0:
                guard let key = self.readLengthPrefixedSlice(data, at: &offset) else { break }
                entries.append(LevelDBEntry(key: key, value: Data(), isDeletion: true))
            case 1:
                guard let key = self.readLengthPrefixedSlice(data, at: &offset),
                      let value = self.readLengthPrefixedSlice(data, at: &offset)
                else { break }
                entries.append(LevelDBEntry(key: key, value: value, isDeletion: false))
            default:
                return entries
            }
        }
        return entries
    }

    // MARK: - Table parsing

    private struct BlockHandle: Sendable {
        let offset: Int
        let size: Int
    }

    private static func readTableEntries(from url: URL, logger: ((String) -> Void)? = nil) -> [LevelDBEntry] {
        guard let data = try? Data(contentsOf: url, options: [.mappedIfSafe]) else { return [] }
        guard data.count >= self.footerSize else { return [] }

        let footerStart = data.count - self.footerSize
        let footerData = data.subdata(in: footerStart..<(data.count - 8))
        var reader = ByteReader(footerData)
        guard self.readBlockHandle(&reader) != nil,
              let indexHandle = self.readBlockHandle(&reader)
        else { return [] }

        guard let indexBlock = self.readBlock(data: data, handle: indexHandle, logger: logger) else { return [] }
        let indexEntries = self.parseDataBlock(indexBlock, treatKeysAsInternal: false)
        var results: [LevelDBEntry] = []

        for entry in indexEntries {
            guard let handle = self.decodeBlockHandle(from: entry.value) else { continue }
            guard let blockData = self.readBlock(data: data, handle: handle, logger: logger) else { continue }
            let dataEntries = self.parseDataBlock(blockData, treatKeysAsInternal: true)
            results.append(contentsOf: dataEntries)
        }
        return results
    }

    private static func readBlock(
        data: Data,
        handle: BlockHandle,
        logger: ((String) -> Void)? = nil) -> Data?
    {
        let start = handle.offset
        let end = handle.offset + handle.size
        guard start >= 0, end + 5 <= data.count else { return nil }
        let rawBlock = data.subdata(in: start..<end)
        let compressionType = data[end]
        switch compressionType {
        case 0:
            return rawBlock
        case 1:
            return SnappyDecoder.decompress(rawBlock)
        default:
            logger?("Unsupported block compression: \(compressionType)")
            return nil
        }
    }

    private static func parseDataBlock(
        _ data: Data,
        treatKeysAsInternal: Bool) -> [LevelDBEntry]
    {
        guard data.count >= 4 else { return [] }
        let restartCount = Int(self.readUInt32LE(data, at: data.count - 4))
        let restartArraySize = (restartCount + 1) * 4
        guard data.count >= restartArraySize else { return [] }
        let limit = data.count - restartArraySize

        var entries: [LevelDBEntry] = []
        var offset = 0
        var lastKey = Data()
        while offset < limit {
            guard let shared = self.readVarint32(data, at: &offset),
                  let nonShared = self.readVarint32(data, at: &offset),
                  let valueLength = self.readVarint32(data, at: &offset)
            else { break }

            let keyEnd = offset + Int(nonShared)
            guard keyEnd <= limit else { break }
            let keySuffix = data.subdata(in: offset..<keyEnd)
            offset = keyEnd

            let valueEnd = offset + Int(valueLength)
            guard valueEnd <= limit else { break }
            let value = data.subdata(in: offset..<valueEnd)
            offset = valueEnd

            let prefix = lastKey.prefix(Int(shared))
            var fullKey = Data(prefix)
            fullKey.append(keySuffix)
            lastKey = fullKey

            if treatKeysAsInternal, let internalKey = self.decodeInternalKey(fullKey) {
                if internalKey.valueType == 0 {
                    entries.append(LevelDBEntry(key: internalKey.userKey, value: Data(), isDeletion: true))
                } else {
                    entries.append(LevelDBEntry(key: internalKey.userKey, value: value, isDeletion: false))
                }
            } else {
                entries.append(LevelDBEntry(key: fullKey, value: value, isDeletion: false))
            }
        }
        return entries
    }

    private static func decodeInternalKey(_ data: Data) -> (userKey: Data, valueType: UInt8)? {
        guard data.count >= 8 else { return nil }
        let userKey = data.prefix(data.count - 8)
        let tag = self.readUInt64LE(data, at: data.count - 8)
        let valueType = UInt8(tag & 0xFF)
        return (Data(userKey), valueType)
    }

    private static func readBlockHandle(_ reader: inout ByteReader) -> BlockHandle? {
        guard let offset = reader.readVarint64(),
              let size = reader.readVarint64()
        else { return nil }
        return BlockHandle(offset: Int(offset), size: Int(size))
    }

    private static func decodeBlockHandle(from value: Data) -> BlockHandle? {
        var reader = ByteReader(value)
        guard let offset = reader.readVarint64(),
              let size = reader.readVarint64()
        else { return nil }
        return BlockHandle(offset: Int(offset), size: Int(size))
    }

    // MARK: - Local storage decoding

    private struct LocalStorageKey: Sendable {
        let origin: String
        let key: String
    }

    private static func decodeLocalStorageKey(_ data: Data) -> LocalStorageKey? {
        if let decoded = self.decodeLocalStorageKey(data, startIndex: 1, requiresPrefix: true) {
            return decoded
        }
        return self.decodeLocalStorageKey(data, startIndex: 0, requiresPrefix: false)
    }

    private static func decodeLocalStorageKey(
        _ data: Data,
        startIndex: Int,
        requiresPrefix: Bool) -> LocalStorageKey?
    {
        let bytes = [UInt8](data)
        if requiresPrefix, bytes.first != 0x5F {
            return nil
        }

        guard let splitIndex = bytes[startIndex...].firstIndex(of: 0x00) else { return nil }
        guard splitIndex + 1 < bytes.count else { return nil }

        let originData = Data(bytes[startIndex..<splitIndex])
        let keyData = Data(bytes[(splitIndex + 1)..<bytes.count])

        guard let originValue = self.decodeText(originData),
              let key = self.decodePrefixedString(keyData) ?? self.decodeText(keyData)
        else { return nil }

        let origin = self.storageKeyOrigin(from: originValue)
        if !requiresPrefix, !self.looksLikeOrigin(origin) {
            return nil
        }
        return LocalStorageKey(origin: origin, key: key)
    }

    private static func looksLikeOrigin(_ value: String) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty { return false }
        if trimmed.contains("://") { return true }
        if trimmed == "localhost" || trimmed.hasPrefix("localhost:") { return true }
        return trimmed.contains(".")
    }

    private static func decodeLocalStorageValue(_ data: Data) -> String? {
        guard !data.isEmpty else { return nil }
        return self.decodePrefixedString(data) ?? self.decodeText(data)
    }

    private static func decodeText(_ data: Data) -> String? {
        if data.isEmpty { return nil }
        if let decoded = self.decodePrefixedString(data) {
            return decoded.trimmingCharacters(in: .controlCharacters)
        }
        if self.looksLikeUTF16(data),
           let decoded = String(data: data, encoding: .utf16LittleEndian)
        {
            return decoded.trimmingCharacters(in: .controlCharacters)
        }
        if let decoded = String(data: data, encoding: .utf8) {
            return decoded.trimmingCharacters(in: .controlCharacters)
        }
        if let decoded = String(data: data, encoding: .utf16LittleEndian) {
            return decoded.trimmingCharacters(in: .controlCharacters)
        }
        if let decoded = String(data: data, encoding: .isoLatin1) {
            return decoded.trimmingCharacters(in: .controlCharacters)
        }
        return nil
    }

    private static func decodePrefixedString(_ data: Data) -> String? {
        guard data.count > 1, let prefix = data.first else { return nil }
        let payload = data.dropFirst()
        switch prefix {
        case 0:
            return String(data: payload, encoding: .utf16LittleEndian)
        case 1:
            return String(data: payload, encoding: .isoLatin1)
        default:
            return nil
        }
    }

    private static func looksLikeUTF16(_ data: Data) -> Bool {
        guard data.count >= 6, data.count % 2 == 0 else { return false }
        let sample = data.prefix(64)
        var zeroCount = 0
        var checked = 0
        var index = 1
        while index < sample.count {
            checked += 1
            if sample[sample.index(sample.startIndex, offsetBy: index)] == 0 {
                zeroCount += 1
            }
            index += 2
        }
        guard checked >= 4 else { return false }
        return Double(zeroCount) / Double(checked) > 0.6
    }

    private static func pickBestValue(_ first: String?, _ second: String?) -> String? {
        let candidates = [first, second].compactMap(\.self).filter { !$0.isEmpty }
        guard let best = candidates.max(by: { $0.count < $1.count }) else { return nil }
        return best
    }

    private static func normalizeOrigin(_ origin: String) -> String {
        let trimmed = origin.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.hasSuffix("/") {
            return String(trimmed.dropLast())
        }
        return trimmed
    }

    private static func storageKeyOrigin(from value: String) -> String {
        // Chromium StorageKey::SerializeForLocalStorage uses origin.Serialize or StorageKey::Serialize
        // (caret-suffixed).
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return trimmed }

        let base: Substring = if let caretIndex = trimmed.firstIndex(of: "^") {
            trimmed[..<caretIndex]
        } else {
            trimmed[...]
        }

        var origin = String(base)
        if let schemeRange = origin.range(of: "://") {
            let afterScheme = origin[schemeRange.upperBound...]
            if let slashIndex = afterScheme.firstIndex(of: "/") {
                origin = String(origin[..<slashIndex])
            }
        } else if let slashIndex = origin.firstIndex(of: "/") {
            origin = String(origin[..<slashIndex])
        }

        if origin.hasSuffix("/") {
            origin.removeLast()
        }
        return origin
    }

    private static func originMatches(_ storageKeyOrigin: String, _ requestedOrigin: String) -> Bool {
        if storageKeyOrigin == requestedOrigin { return true }

        let storageHost = self.originHost(from: storageKeyOrigin)
        let requestedHost = self.originHost(from: requestedOrigin)
        if let storageHost, let requestedHost, storageHost == requestedHost { return true }

        let requestedStripped = self.stripScheme(from: requestedOrigin)
        if storageKeyOrigin == requestedStripped { return true }
        return false
    }

    private static func originHost(from value: String) -> String? {
        if let url = URL(string: value), let host = url.host {
            if let port = url.port {
                return "\(host):\(port)"
            }
            return host
        }
        let stripped = self.stripScheme(from: value)
        let host = stripped.split(separator: "/").first
        return host.map(String.init)
    }

    private static func stripScheme(from value: String) -> String {
        if let range = value.range(of: "://") {
            return String(value[range.upperBound...])
        }
        return value
    }

    private static func scanTokens(in data: Data, minimumLength: Int) -> [String] {
        guard minimumLength > 0 else { return [] }
        var buffer: [UInt8] = []
        var results: [String] = []
        buffer.reserveCapacity(minimumLength)

        func flushBuffer() {
            guard buffer.count >= minimumLength,
                  let string = String(bytes: buffer, encoding: .ascii)
            else {
                buffer.removeAll(keepingCapacity: true)
                return
            }
            let parts = string.split(separator: ".")
            if parts.count >= 3 || string.count >= minimumLength {
                results.append(string)
            }
            buffer.removeAll(keepingCapacity: true)
        }

        for byte in data {
            if self.tokenBytes.contains(byte) {
                buffer.append(byte)
            } else if !buffer.isEmpty {
                flushBuffer()
            }
        }
        if !buffer.isEmpty {
            flushBuffer()
        }

        return results
    }

    // MARK: - Data helpers

    private struct ByteReader {
        private let bytes: [UInt8]
        private(set) var index: Int = 0

        init(_ data: Data) {
            self.bytes = Array(data)
        }

        mutating func readVarint64() -> UInt64? {
            var result: UInt64 = 0
            var shift: UInt64 = 0
            while shift < 64 {
                guard let byte = self.readUInt8() else { return nil }
                result |= UInt64(byte & 0x7F) << shift
                if (byte & 0x80) == 0 {
                    return result
                }
                shift += 7
            }
            return nil
        }

        mutating func readUInt8() -> UInt8? {
            guard self.index < self.bytes.count else { return nil }
            let value = self.bytes[self.index]
            self.index += 1
            return value
        }
    }

    private static func readUInt8(_ data: Data, at offset: inout Int) -> UInt8? {
        guard offset < data.count else { return nil }
        let value = data[offset]
        offset += 1
        return value
    }

    private static func readUInt16LE(_ data: Data, at offset: Int) -> UInt16 {
        let byte0 = UInt16(data[offset])
        let byte1 = UInt16(data[offset + 1])
        return byte0 | (byte1 << 8)
    }

    private static func readUInt32LE(_ data: Data, at offset: Int) -> UInt32 {
        let byte0 = UInt32(data[offset])
        let byte1 = UInt32(data[offset + 1]) << 8
        let byte2 = UInt32(data[offset + 2]) << 16
        let byte3 = UInt32(data[offset + 3]) << 24
        return byte0 | byte1 | byte2 | byte3
    }

    private static func readUInt64LE(_ data: Data, at offset: Int) -> UInt64 {
        var value: UInt64 = 0
        for index in 0..<8 {
            value |= UInt64(data[offset + index]) << (UInt64(index) * 8)
        }
        return value
    }

    private static func readVarint32(_ data: Data, at offset: inout Int) -> UInt32? {
        var result: UInt32 = 0
        var shift: UInt32 = 0
        while shift < 32 {
            guard let byte = self.readUInt8(data, at: &offset) else { return nil }
            result |= UInt32(byte & 0x7F) << shift
            if (byte & 0x80) == 0 {
                return result
            }
            shift += 7
        }
        return nil
    }

    private static func readLengthPrefixedSlice(_ data: Data, at offset: inout Int) -> Data? {
        guard let length = self.readVarint32(data, at: &offset) else { return nil }
        let count = Int(length)
        guard count >= 0, offset + count <= data.count else { return nil }
        let slice = data.subdata(in: offset..<(offset + count))
        offset += count
        return slice
    }
}

#endif
