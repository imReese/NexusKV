# Appendix F: Figure Index and Rendering Notes

This appendix indexes the figures embedded in the
[NexusKV Whitepaper v1.0](a-survey-of-kv-cache-systems-for-llm-inference.md).
The main paper is the canonical location for captions and interpretation.

## F.1 Figure index

| Figure | Asset | First body reference | Purpose |
| --- | --- | --- | --- |
| 1 | [`kv-cache-evolution.svg`](figures/kv-cache-evolution.svg) | Section 1 | Show the expansion from request-local KV buffers to coordinated Model State. |
| 2 | [`kv-cache-research-landscape.svg`](figures/kv-cache-research-landscape.svg) | Section 3 | Place representative systems by primary architectural responsibility. |
| 3 | [`zero-overhead-pipeline.svg`](figures/zero-overhead-pipeline.svg) | Section 6 | Distinguish fully hidden, partially visible, and abandoned transfer work. |
| 4 | [`nexuskv-zero-overhead-architecture.svg`](figures/nexuskv-zero-overhead-architecture.svg) | Section 8 | Show the Intelligence Layer between Inference Runtimes and Data Plane components. |

## F.2 Rendering requirements

- Use repository-relative paths so figures render on GitHub and in repository
  clones.
- Keep the figure number and explanatory caption in Markdown; do not rely on
  text embedded in the SVG as the only explanation.
- Give every SVG a `<title>`, `<desc>`, and `role="img"`.
- Preserve readable contrast in light and dark GitHub themes.
- Treat project placement as an architectural scope map, not a performance
  ranking.
- Update the body reference, caption, and this index together when figure order
  changes.

## F.3 Release check

Before release, parse every SVG as XML, render it to a raster preview, inspect
labels at GitHub content width, and verify that each figure is referenced in
the body before it appears.
