# Lanhu Asset Exporter

A terminal UI for exporting Lanhu project pages without executing pasted shell commands. Paste the
`/api/project/images` curl request, choose an asset mode, and export Sketch JSON, previews and
development cutouts into `./lanhu_export/<project>/`.

## Run

```sh
cargo run --release
```

Paste the list curl into the first panel. Use `Tab` to select asset or version mode and the arrow
keys to change it. Press `Enter` from any panel to export; `Ctrl+U` clears the curl input. The exporter
runs up to `min(available CPU cores, 6)` page pipelines concurrently. Credentials are used only in
memory and are never written to manifests or logs.

## Asset Modes

- **Lanhu cutouts**: default mode; only downloads `image.imageUrl` from `exportable: true` layers,
  then generates PNG `1x`, `@2x`, and `@3x` variants from each original. This most closely matches
  Lanhu's iOS PNG cutout download and excludes system/decorative layers.
- **Reconstruction assets**: downloads all explicit SVG and PNG resources to support visual page recreation.
- **Full layers**: adds Lanhu `ddsImage` layer renderings, including non-exportable layers, for visual auditing.
