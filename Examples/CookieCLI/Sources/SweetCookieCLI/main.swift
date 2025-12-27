import Foundation
import SweetCookieKit

// Example CLI that lists cookie stores and exports cookie records via SweetCookieKit.

@main
enum SweetCookieCLI {
    static func main() {
        do {
            try run()
        } catch {
            log("error: \(error.localizedDescription)")
            exit(1)
        }
    }

    private static func run() throws {
        let options = try Options.parse(CommandLine.arguments)
        if options.showHelp {
            printHelp()
            return
        }

        if options.listBrowsers {
            listBrowsers()
            return
        }

        let browsers = try resolveBrowsers(from: options)
        let client = BrowserCookieClient()

        if options.listStores {
            listStores(client: client, browsers: browsers, options: options)
            return
        }

        // Build query used for all selected stores.
        let query = BrowserCookieQuery(
            domains: options.domains,
            domainMatch: options.domainMatch,
            includeExpired: options.includeExpired)

        let stores = selectedStores(client: client, browsers: browsers, options: options)
        let storeRecords = loadRecords(client: client, stores: stores, query: query)

        // Render selected output format.
        switch options.format {
        case .json:
            try writeJSON(stores: storeRecords)
        case .lines:
            writeLines(stores: storeRecords)
        case .cookieHeader:
            writeCookieHeaders(stores: storeRecords)
        }
    }

    private static func listBrowsers() {
        for browser in Browser.allCases {
            print("\(browser.rawValue)\t\(browser.displayName)")
        }
    }

    private static func listStores(
        client: BrowserCookieClient,
        browsers: [Browser],
        options: Options)
    {
        let stores = selectedStores(client: client, browsers: browsers, options: options)
        if stores.isEmpty {
            log("warning: no stores found for selection")
            return
        }
        for store in stores {
            print("\(store.browser.rawValue)\t\(store.profile.name)\t\(store.profile.id)\t\(store.kind.rawValue)\t\(store.label)")
        }
    }

    // Resolve stores based on browser/profile/kind filters.
    private static func selectedStores(
        client: BrowserCookieClient,
        browsers: [Browser],
        options: Options)
        -> [BrowserCookieStore]
    {
        var stores = browsers.flatMap { client.stores(for: $0) }
        if let profile = options.profile {
            stores = stores.filter { $0.profile.name.caseInsensitiveCompare(profile) == .orderedSame }
        }
        if let kind = options.kind {
            stores = stores.filter { $0.kind == kind }
        }
        if stores.isEmpty {
            for browser in browsers {
                let available = client.stores(for: browser)
                if available.isEmpty {
                    log("warning: no stores for \(browser.displayName)")
                }
            }
        }
        return stores
    }

    // Load records per store, keeping partial results even on failure.
    private static func loadRecords(
        client: BrowserCookieClient,
        stores: [BrowserCookieStore],
        query: BrowserCookieQuery)
        -> [BrowserCookieStoreRecords]
    {
        var results: [BrowserCookieStoreRecords] = []
        for store in stores {
            do {
                let records = try client.records(matching: query, in: store) {
                    log($0)
                }
                results.append(BrowserCookieStoreRecords(store: store, records: records))
            } catch {
                log("warning: \(store.browser.displayName) \(store.label): \(error.localizedDescription)")
            }
        }
        return results
    }

    // Structured JSON payload for automation.
    private static func writeJSON(stores: [BrowserCookieStoreRecords]) throws {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

        let payload = OutputPayload(
            generatedAt: formatter.string(from: Date()),
            stores: stores.map { store in
                OutputStore(
                    browser: store.store.browser.rawValue,
                    browserDisplayName: store.store.browser.displayName,
                    profileId: store.store.profile.id,
                    profileName: store.store.profile.name,
                    kind: store.store.kind.rawValue,
                    label: store.store.label,
                    records: store.records.map { record in
                        OutputRecord(
                            domain: record.domain,
                            name: record.name,
                            value: record.value,
                            path: record.path,
                            expires: record.expires.map { formatter.string(from: $0) },
                            isSecure: record.isSecure,
                            isHTTPOnly: record.isHTTPOnly)
                    })
            })

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let data = try encoder.encode(payload)
        if let text = String(data: data, encoding: .utf8) {
            print(text)
        }
    }

    private static func writeLines(stores: [BrowserCookieStoreRecords]) {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        for store in stores {
            print("# \(store.store.browser.displayName) - \(store.store.profile.name) [\(store.store.kind.rawValue)]")
            for record in store.records {
                let expires = record.expires.map { formatter.string(from: $0) } ?? ""
                print("\(record.domain)\t\(record.name)\t\(record.value)\t\(record.path)\t\(expires)\t\(record.isSecure)\t\(record.isHTTPOnly)")
            }
        }
    }

    private static func writeCookieHeaders(stores: [BrowserCookieStoreRecords]) {
        for store in stores {
            let grouped = Dictionary(grouping: store.records, by: { $0.domain })
            for domain in grouped.keys.sorted() {
                guard let records = grouped[domain], !records.isEmpty else { continue }
                let header = records.map { "\($0.name)=\($0.value)" }.joined(separator: "; ")
                print("# \(store.store.browser.displayName) - \(store.store.profile.name) - \(domain)")
                print("Cookie: \(header)")
                print("")
            }
        }
    }

    private static func resolveBrowsers(from options: Options) throws -> [Browser] {
        if options.allBrowsers {
            return Browser.allCases
        }
        if options.browserTokens.isEmpty {
            return BrowserCookieDefaults.importOrder
        }
        var seen = Set<Browser>()
        var resolved: [Browser] = []
        for token in options.browserTokens {
            if let browser = resolveBrowser(token) {
                if !seen.contains(browser) {
                    resolved.append(browser)
                    seen.insert(browser)
                }
            } else {
                throw CLIError.invalidValue("browser", token)
            }
        }
        return resolved
    }

    private static func resolveBrowser(_ token: String) -> Browser? {
        let normalizedToken = normalize(token)
        return Browser.allCases.first {
            normalize($0.rawValue) == normalizedToken || normalize($0.displayName) == normalizedToken
        }
    }

    private static func printHelp() {
        print(
            """
            SweetCookieCLI - extract browser cookies using SweetCookieKit.

            USAGE:
              SweetCookieCLI [options]

            OPTIONS:
              --domains <a,b,c>       Domain filters (comma-separated)
              --domain <value>        Domain filter (repeatable)
              --domain-match <mode>   contains | suffix | exact (default: contains)
              --browser <name>        Browser raw name (repeatable, comma-separated)
              --all-browsers          Use all supported browsers
              --profile <name>        Profile name filter
              --kind <value>          primary | network | safari
              --include-expired       Include expired cookies
              --format <value>        json | lines | cookie-header (default: json)
              --list-browsers         List browser identifiers
              --list-stores           List stores for selection
              -h, --help              Show help

            EXAMPLES:
              SweetCookieCLI --domains example.com --browser chrome --profile Default
              SweetCookieCLI --domain example.com --format cookie-header
              SweetCookieCLI --list-browsers
              SweetCookieCLI --list-stores --browser safari
            """
        )
    }

    private static func log(_ message: String) {
        fputs("\(message)\n", stderr)
    }
}

// Parsed CLI arguments with defaults.
private struct Options {
    var domains: [String] = []
    var domainMatch: BrowserCookieDomainMatch = .contains
    var includeExpired = false
    var browserTokens: [String] = []
    var allBrowsers = false
    var profile: String?
    var kind: BrowserCookieStoreKind?
    var format: OutputFormat = .json
    var listBrowsers = false
    var listStores = false
    var showHelp = false

    static func parse(_ arguments: [String]) throws -> Options {
        var options = Options()
        var index = 1
        while index < arguments.count {
            let arg = arguments[index]
            switch arg {
            case "-h", "--help":
                options.showHelp = true
            case "--domains":
                let value = try consumeValue(from: arguments, index: &index, flag: arg)
                options.domains.append(contentsOf: splitList(value))
            case "--domain":
                let value = try consumeValue(from: arguments, index: &index, flag: arg)
                options.domains.append(value)
            case "--domain-match":
                let value = try consumeValue(from: arguments, index: &index, flag: arg)
                options.domainMatch = try parseDomainMatch(value)
            case "--browser":
                let value = try consumeValue(from: arguments, index: &index, flag: arg)
                let tokens = splitList(value)
                if tokens.contains(where: { normalize($0) == "all" }) {
                    options.allBrowsers = true
                }
                options.browserTokens.append(contentsOf: tokens.filter { normalize($0) != "all" })
            case "--all-browsers":
                options.allBrowsers = true
            case "--profile":
                let value = try consumeValue(from: arguments, index: &index, flag: arg)
                options.profile = value
            case "--kind":
                let value = try consumeValue(from: arguments, index: &index, flag: arg)
                guard let kind = BrowserCookieStoreKind(rawValue: value) else {
                    throw CLIError.invalidValue("kind", value)
                }
                options.kind = kind
            case "--include-expired":
                options.includeExpired = true
            case "--format":
                let value = try consumeValue(from: arguments, index: &index, flag: arg)
                guard let format = OutputFormat(rawValue: value) else {
                    throw CLIError.invalidValue("format", value)
                }
                options.format = format
            case "--list-browsers":
                options.listBrowsers = true
            case "--list-stores":
                options.listStores = true
            default:
                if arg.hasPrefix("-") {
                    throw CLIError.unknownOption(arg)
                }
                options.domains.append(arg)
            }
            index += 1
        }
        return options
    }
}

// Output shapes supported by the CLI.
private enum OutputFormat: String {
    case json
    case lines
    case cookieHeader = "cookie-header"
}

private enum CLIError: LocalizedError {
    case unknownOption(String)
    case missingValue(String)
    case invalidValue(String, String)

    var errorDescription: String? {
        switch self {
        case let .unknownOption(option):
            "Unknown option: \(option)"
        case let .missingValue(flag):
            "Missing value for \(flag)"
        case let .invalidValue(flag, value):
            "Invalid value for \(flag): \(value)"
        }
    }
}

// JSON output payload.
private struct OutputPayload: Codable {
    let generatedAt: String
    let stores: [OutputStore]
}

// Per-store JSON output.
private struct OutputStore: Codable {
    let browser: String
    let browserDisplayName: String
    let profileId: String
    let profileName: String
    let kind: String
    let label: String
    let records: [OutputRecord]
}

// Per-cookie JSON output.
private struct OutputRecord: Codable {
    let domain: String
    let name: String
    let value: String
    let path: String
    let expires: String?
    let isSecure: Bool
    let isHTTPOnly: Bool
}

private func splitList(_ value: String) -> [String] {
    value
        .split(separator: ",")
        .map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) }
        .filter { !$0.isEmpty }
}

private func parseDomainMatch(_ value: String) throws -> BrowserCookieDomainMatch {
    switch value {
    case "contains":
        .contains
    case "suffix":
        .suffix
    case "exact":
        .exact
    default:
        throw CLIError.invalidValue("domain-match", value)
    }
}

private func consumeValue(from arguments: [String], index: inout Int, flag: String) throws -> String {
    let nextIndex = index + 1
    guard nextIndex < arguments.count else {
        throw CLIError.missingValue(flag)
    }
    index = nextIndex
    return arguments[nextIndex]
}

private func normalize(_ value: String) -> String {
    value
        .lowercased()
        .replacingOccurrences(of: "-", with: "")
        .replacingOccurrences(of: "_", with: "")
        .replacingOccurrences(of: " ", with: "")
}
