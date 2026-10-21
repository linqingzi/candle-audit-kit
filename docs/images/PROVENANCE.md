# Image and report provenance

These are real browser captures of generated synthetic reports, with no real market data,
trading accounts, AI-generated report pixels or invented application interface.

## English capture: new on main

The original tool produced Chinese reports only. Optional English HTML display was added on
main after 0.1.0rc1; this screenshot demonstrates that new implementation, not a feature of
the unchanged 0.1.0rc1 archive. Run the current source with:

```sh
candle-audit demo --root data/demo --language en
```

Then open the HTML file printed as `report.html_path`. This actual run completed with
`SYNTHETIC_DEMO_COMPLETE`, 8 hourly rows, 2 complete four-hour rows, `network_used: false`,
`source_verification: UNVERIFIED` and `admission: NOT_EVALUATED`. Prices and rules are invented
engineering inputs. IDs can differ in later runs because capture evidence is versioned.

- `synthetic-report.en.jpg`: unmodified browser viewport screenshot, 931 × 1178 pixels.
  The report labels, chart labels, controls, quality dimensions and evidence notices are English.
- `../examples/synthetic-report.en.html`: the generated self-contained English HTML, copied
  byte-for-byte. SHA256: `da00efc7d47e192d4bd4cb4463716a5e994876d86ef205eac016d2349cb8c98e`.
- `social-preview.jpg`: 1280 × 640 capture of `social-preview.html`, now composing English
  project text with the same real English report screenshot. The frame crops for layout;
  it does not change values or evidence states. This file does not prove a repository social
  preview setting has been applied.
- `social-preview.html`: dependency-free HTML/CSS source for that share card.

Only project-owned display labels are translated. Exact values, dataset names, evidence
statuses and JSON report schema/text are retained. Chinese remains the default. Neither
`UNVERIFIED` nor `UNKNOWN` becomes verified through translation.

## Original Chinese capture: retained

Run `candle-audit demo --root data/demo` without a language option for the Chinese view.
The original Chinese capture used the validated 0.1.0rc1 implementation; its files remain
unchanged, and the Chinese README still uses that image.

- `synthetic-report.jpg`: original 931 × 1123 browser viewport screenshot.
- `../examples/synthetic-report.html`: original generated HTML, preserved byte-for-byte.
  SHA256: `2df8f2367dc69cc4332395b073a2124d9ea2b72a6a097e61d780bb9382168968`.

All JPEGs are below 1 MB and contain no EXIF segment. They were captured on 2026-10-21 from
locally served files, without a browser address bar, private path or account information.
No local URL is needed for the published assets, and no viewport or browser security setting
was changed. Project documentation/images follow the root MIT license; third-party copyright
and license files are unchanged.
