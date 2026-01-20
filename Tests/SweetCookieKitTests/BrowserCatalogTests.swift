import Testing
@testable import SweetCookieKit

#if os(macOS)

@Suite
struct BrowserCatalogTests {
    @Test
    func metadata_coversAllBrowsers() {
        #expect(BrowserCatalog.metadataByBrowser.count == Browser.allCases.count)

        for browser in Browser.allCases {
            let metadata = BrowserCatalog.metadata(for: browser)
            #expect(!metadata.displayName.isEmpty)
        }
    }

    @Test
    func defaultImportOrder_containsAllBrowsers() {
        let order = BrowserCatalog.defaultImportOrder
        #expect(order.count == Browser.allCases.count)
        #expect(Set(order) == Set(Browser.allCases))
        #expect(Set(order).count == order.count)
    }

    @Test
    func chromiumProfileRelativePath_presentForChromium() {
        for browser in Browser.allCases where browser.engine == .chromium {
            let path = BrowserCatalog.metadata(for: browser).chromiumProfileRelativePath
            #expect(path != nil)
        }
    }

    @Test
    func geckoProfilesFolder_presentForGecko() {
        for browser in Browser.allCases where browser.engine == .gecko {
            let folder = BrowserCatalog.metadata(for: browser).geckoProfilesFolder
            #expect(folder != nil)
        }
    }
}

#endif
