# AI Design Handoff

Project: {{PROJECT_NAME}}

This export contains {{PAGE_COUNT}} pages, {{ASSET_COUNT}} selected assets, and {{VARIANT_COUNT}} asset variants.
Asset mode: {{ASSET_MODE}}

## Files

- `manifest.json`: page index and the authoritative mapping from design layers to local assets.
- `pages/<page>-<id>/versions/<version>/sketch.json`: layout, layer tree, text, colors, typography, and effects.
- `pages/<page>-<id>/versions/<version>/preview.png`: visual reference for validation only.
- `pages/<page>-<id>/versions/<version>/assets/`: local assets referenced by `manifest.json`.

## Required Workflow

1. Read `manifest.json` before selecting or implementing a page.
2. Use `sketch.json` as the source of truth for logical layout: `left`, `top`, `width`, and `height` are design-point values. Do not use image pixel dimensions as layout values.
3. Recreate text, shapes, gradients, colors, and layout with native UI code whenever the Sketch JSON contains enough information.
4. Use `preview.png` only to compare the implemented screen with the source design. Do not use it as a full-screen background or as the implementation itself.
5. Use assets only through the matching `manifest.json` entry. For assets with `variants`, use `scale: 1`, `scale: 2`, and `scale: 3` as the iOS 1x, 2x, and 3x files.
6. A page with zero assets is not blank. Continue parsing its Sketch JSON for text, shape, gradient, bitmap, and group layers.
7. Preserve the supplied page geometry before making responsive adaptations. State any necessary adaptation explicitly.

## Constraints

- Do not invent API contracts, navigation, animation timing, or business rules that are absent from the design export. List assumptions instead.
- Do not substitute arbitrary icons or stock imagery when a matching local asset or vector layer exists.
- Do not upscale a low-resolution raster asset to claim it is a high-resolution source. Prefer a matching SVG when available.
- Reuse shared visual components only after verifying they are visually identical across pages.

## Completion Check

For every implemented page, compare a simulator or browser screenshot against the corresponding `preview.png` and correct visible differences in layout, typography, color, radius, and spacing.

## Current Export Semantics

{{MODE_GUIDANCE}}
