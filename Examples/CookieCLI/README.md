# SweetCookieCLI

Example CLI that uses SweetCookieKit to list stores and export cookies as JSON or headers.

## Build and run

```bash
cd Examples/CookieCLI
swift run SweetCookieCLI --help
```

## Examples

```bash
swift run SweetCookieCLI --list-browsers
swift run SweetCookieCLI --list-stores --browser chrome
swift run SweetCookieCLI --domains example.com --browser chrome --profile Default
swift run SweetCookieCLI --domain example.com --format cookie-header
```

## Notes

- Safari cookie access may require Full Disk Access.
- Chromium imports can trigger a Keychain prompt for "Chrome Safe Storage".
