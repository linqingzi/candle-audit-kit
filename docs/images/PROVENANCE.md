# Image and report provenance

These assets show an actual run of Candle Audit Kit 0.1.0rc1. They do not use real exchange data, trading accounts, AI-generated report pixels or an invented application interface.

## Reproduce the report

Run `candle-audit demo --root data/demo` in a fresh writable workspace, then open the HTML file printed as `report.html_path` in a browser. The packaged inputs are also in `examples/synthetic/`.

This capture completed with `SYNTHETIC_DEMO_COMPLETE`, 8 hourly rows, 2 complete four-hour rows, `network_used: false`, `source_verification: UNVERIFIED` and `admission: NOT_EVALUATED`. Prices and rules are invented engineering inputs. IDs may differ in later runs because capture evidence is versioned.

- `synthetic-report.jpg`: an unmodified browser viewport screenshot of the real generated report, 931 × 1123 pixels. The screenshot contains no browser address bar, private path or account information.
- `../examples/synthetic-report.html`: the corresponding self-contained generated HTML, preserved byte-for-byte. SHA256: `2df8f2367dc69cc4332395b073a2124d9ea2b72a6a097e61d780bb9382168968`.
- `social-preview.jpg`: 1280 × 640 pixels, captured from `social-preview.html`. It combines descriptive project text with the same real report screenshot. The frame crops the image for the card layout; it does not change the report's values or evidence states.
- `social-preview.html`: dependency-free HTML/CSS source for the share card. Serve this docs directory locally and capture its 1280 × 640 card area; no viewport or browser security setting needs to be changed.

Both JPEG files are below 1 MB and contain no EXIF entries. Screenshots were captured on 2026-10-21 from locally served files; no local URL is required for the published images.

The runtime Python source was unchanged from the validated 0.1.0rc1 baseline when these assets were generated. The chart's instrument label identifies the input schema, not a claim of real market observations. `UNVERIFIED` and `UNKNOWN` remain meaningful limitations. Project-owned documentation and images follow the root MIT license; dependency copyright and license files are unchanged.
