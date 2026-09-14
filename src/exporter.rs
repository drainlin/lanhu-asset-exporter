use std::collections::HashSet;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;

use anyhow::{Context, Result, bail};
use futures::{StreamExt, stream};
use reqwest::header::{CONTENT_TYPE, HeaderMap};
use reqwest::{Client, Url};
use serde_json::Value;

use crate::curl::{CurlRequest, is_lanhu_host};
use crate::model::{
    AssetManifest, AssetMode, AssetVariantManifest, ExportManifest, ExportOptions, PageManifest,
    ProgressEvent, VersionManifest, VersionMode,
};

pub async fn run(
    request: CurlRequest,
    options: ExportOptions,
    tx: Sender<ProgressEvent>,
) -> Result<()> {
    let client = Client::builder()
        .user_agent("lanhu-asset-exporter/0.1")
        .build()?;
    let list: Value = get_json(&client, request.list_url.clone(), &request.headers).await?;
    let project_name = list
        .pointer("/data/name")
        .and_then(Value::as_str)
        .unwrap_or("lanhu-project");
    let images = list
        .pointer("/data/images")
        .and_then(Value::as_array)
        .context("list response has no data.images array")?;
    let project_id = query_value(&request.list_url, "project_id")?;
    let team_id = query_value(&request.list_url, "team_id")?;
    let output = std::env::current_dir()?
        .join("lanhu_export")
        .join(safe_name(project_name));
    fs::create_dir_all(&output)?;
    let pages: Vec<(String, String)> = images
        .iter()
        .filter_map(|image| {
            let id = image.get("id")?.as_str()?;
            let name = image.get("name").and_then(Value::as_str).unwrap_or(id);
            Some((id.to_owned(), name.to_owned()))
        })
        .collect();
    tx.send(ProgressEvent::Started {
        project: project_name.to_owned(),
        pages: pages.len(),
    })
    .ok();

    let mut manifest = ExportManifest {
        project: project_name.to_owned(),
        pages: Vec::new(),
    };
    let mut failures = Vec::new();
    let mut done = 0;
    let total = pages.len();

    let page_stream = stream::iter(pages.into_iter().map(|(id, name)| {
        let client = client.clone();
        let headers = request.headers.clone();
        let project_id = project_id.clone();
        let team_id = team_id.clone();
        let output = output.clone();
        let options = options.clone();
        let tx = tx.clone();
        async move {
            tx.send(ProgressEvent::Status(format!("Loading {name}")))
                .ok();
            let result = export_page(
                &client,
                &headers,
                &project_id,
                &team_id,
                &id,
                &name,
                &output,
                &options,
                &tx,
            )
            .await;
            (id, name, result)
        }
    }))
    .buffer_unordered(options.concurrency.max(1));

    futures::pin_mut!(page_stream);
    while let Some((id, name, result)) = page_stream.next().await {
        match result {
            Ok((page, _downloads)) => manifest.pages.push(page),
            Err(error) => failures.push(format!("{name} ({id}): {error:#}")),
        }
        done += 1;
        tx.send(ProgressEvent::Progress { done, total }).ok();
    }

    fs::write(
        output.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    if !failures.is_empty() {
        fs::write(
            output.join("failures.json"),
            serde_json::to_vec_pretty(&failures)?,
        )?;
    }
    fs::write(
        output.join("AI_HANDOFF.md"),
        render_ai_handoff(&manifest, options.asset_mode),
    )?;
    tx.send(ProgressEvent::Finished {
        output: output.display().to_string(),
        failures: failures.len(),
    })
    .ok();
    Ok(())
}

fn render_ai_handoff(manifest: &ExportManifest, mode: AssetMode) -> String {
    const TEMPLATE: &str = include_str!("../AI_HANDOFF.template.md");
    let asset_count = manifest
        .pages
        .iter()
        .flat_map(|page| &page.versions)
        .map(|version| version.assets.len())
        .sum::<usize>();
    let variant_count = manifest
        .pages
        .iter()
        .flat_map(|page| &page.versions)
        .flat_map(|version| &version.assets)
        .map(|asset| asset.variants.len())
        .sum::<usize>();
    TEMPLATE
        .replace("{{PROJECT_NAME}}", &manifest.project)
        .replace("{{PAGE_COUNT}}", &manifest.pages.len().to_string())
        .replace("{{ASSET_COUNT}}", &asset_count.to_string())
        .replace("{{VARIANT_COUNT}}", &variant_count.to_string())
        .replace("{{ASSET_MODE}}", mode.label())
        .replace("{{MODE_GUIDANCE}}", mode_guidance(mode))
}

fn mode_guidance(mode: AssetMode) -> &'static str {
    match mode {
        AssetMode::Cutouts => {
            "This is the Lanhu cutout mode. It includes only `exportable: true` PNG resources and generates local 1x, 2x, and 3x PNG variants. Decorative and system layers are intentionally excluded."
        }
        AssetMode::Smart => {
            "This is the reconstruction-assets mode. It includes explicit SVG and PNG resources from all layers, but excludes DDS layer renderings."
        }
        AssetMode::FullLayers => {
            "This is the full-layers mode. It includes explicit SVG/PNG resources and DDS renderings for non-text layers; treat DDS assets as visual references rather than product assets."
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn export_page(
    client: &Client,
    headers: &HeaderMap,
    project_id: &str,
    team_id: &str,
    image_id: &str,
    name: &str,
    output: &Path,
    options: &ExportOptions,
    tx: &Sender<ProgressEvent>,
) -> Result<(PageManifest, usize)> {
    let mut detail_url = Url::parse("https://lanhuapp.com/api/project/image")?;
    detail_url.query_pairs_mut().extend_pairs([
        ("dds_status", "1"),
        ("image_id", image_id),
        ("team_id", team_id),
        ("project_id", project_id),
        (
            "all_versions",
            if options.version_mode == VersionMode::All {
                "1"
            } else {
                "0"
            },
        ),
    ]);
    let detail = get_json(client, detail_url, headers).await?;
    let result = detail
        .get("result")
        .context("detail response has no result")?;
    let latest = result
        .get("latest_version")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let versions = result
        .get("versions")
        .and_then(Value::as_array)
        .context("detail response has no versions")?;
    let page_dir = output
        .join("pages")
        .join(format!("{}-{}", safe_name(name), short_id(image_id)));
    fs::create_dir_all(&page_dir)?;
    let preview = result.get("url").and_then(Value::as_str).map(str::to_owned);
    let mut version_entries = Vec::new();
    let mut download_count = 0;

    for version in versions {
        let version_id = version
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        if options.version_mode == VersionMode::Latest && version_id != latest {
            continue;
        }
        let json_url = version
            .get("json_url")
            .and_then(Value::as_str)
            .context("version has no json_url")?;
        let sketch = get_json(client, checked_url(json_url)?, headers).await?;
        let version_label = safe_name(
            version
                .get("version_info")
                .and_then(Value::as_str)
                .unwrap_or(version_id),
        );
        let version_dir = page_dir
            .join("versions")
            .join(format!("{version_label}-{}", short_id(version_id)));
        fs::create_dir_all(version_dir.join("assets"))?;
        fs::write(
            version_dir.join("sketch.json"),
            serde_json::to_vec_pretty(&sketch)?,
        )?;
        let mut assets = Vec::new();
        let mut seen = HashSet::new();
        collect_assets(&sketch, options.asset_mode, &mut assets, &mut seen);
        let mut asset_entries = Vec::new();
        for (index, asset) in assets.iter().enumerate() {
            tx.send(ProgressEvent::Status(format!("{}: {}", name, asset.name)))
                .ok();
            let local = version_dir.join("assets").join(format!(
                "{:02}-{}",
                index + 1,
                safe_name(&asset.name)
            ));
            let bytes = get_bytes(client, checked_url(&asset.url)?, headers).await?;
            let (local, variants) = if options.asset_mode == AssetMode::Cutouts {
                let variants = write_ios_variants(&local, &bytes.body, asset.width, asset.height)
                    .with_context(|| {
                    format!("cannot generate iOS variants for {}", asset.name)
                })?;
                let local = variants
                    .first()
                    .context("iOS variant generation returned no 1x asset")?
                    .local_path
                    .clone();
                (local, variants)
            } else {
                let local = local.with_extension(extension_for(asset.kind, &bytes.content_type));
                fs::write(&local, bytes.body)?;
                (local, Vec::new())
            };
            asset_entries.push(AssetManifest {
                layer_name: asset.name.clone(),
                kind: asset.kind.to_owned(),
                local_path: local.strip_prefix(output)?.display().to_string(),
                variants: variants
                    .into_iter()
                    .map(|variant| AssetVariantManifest {
                        scale: variant.scale,
                        local_path: variant
                            .local_path
                            .strip_prefix(output)
                            .expect("variant is inside the export directory")
                            .display()
                            .to_string(),
                        pixel_width: variant.pixel_width,
                        pixel_height: variant.pixel_height,
                    })
                    .collect(),
                width: asset.width,
                height: asset.height,
                x: asset.x,
                y: asset.y,
            });
            download_count += 1;
        }
        let preview_path = if let Some(url) = preview.as_deref() {
            let bytes = get_bytes(client, checked_url(url)?, headers).await?;
            let path = version_dir
                .join("preview")
                .with_extension(extension_for("preview", &bytes.content_type));
            fs::write(&path, bytes.body)?;
            Some(path.strip_prefix(output)?.display().to_string())
        } else {
            None
        };
        version_entries.push(VersionManifest {
            version_id: version_id.to_owned(),
            sketch_json: version_dir
                .join("sketch.json")
                .strip_prefix(output)?
                .display()
                .to_string(),
            preview: preview_path,
            assets: asset_entries,
        });
    }
    Ok((
        PageManifest {
            image_id: image_id.to_owned(),
            name: name.to_owned(),
            versions: version_entries,
        },
        download_count,
    ))
}

struct Asset {
    name: String,
    url: String,
    kind: &'static str,
    width: Option<f64>,
    height: Option<f64>,
    x: Option<f64>,
    y: Option<f64>,
}
struct Download {
    body: Vec<u8>,
    content_type: Option<String>,
}

struct IosVariant {
    scale: u8,
    local_path: PathBuf,
    pixel_width: u32,
    pixel_height: u32,
}

fn write_ios_variants(
    local_base: &Path,
    source: &[u8],
    logical_width: Option<f64>,
    logical_height: Option<f64>,
) -> Result<Vec<IosVariant>> {
    let logical_width = logical_width.context("layer has no logical width")?;
    let logical_height = logical_height.context("layer has no logical height")?;
    let name = local_base
        .file_name()
        .and_then(|name| name.to_str())
        .context("asset file name is not valid UTF-8")?;
    let mut variants = Vec::with_capacity(3);
    for scale in 1..=3 {
        let pixel_width = scaled_pixels(logical_width, scale);
        let pixel_height = scaled_pixels(logical_height, scale);
        let bytes = render_ios_variant(source, pixel_width, pixel_height)?;
        let filename = if scale == 1 {
            format!("{name}.png")
        } else {
            format!("{name}@{scale}x.png")
        };
        let local_path = local_base.with_file_name(&filename);
        fs::write(&local_path, bytes)?;
        variants.push(IosVariant {
            scale,
            local_path,
            pixel_width,
            pixel_height,
        });
    }
    Ok(variants)
}

fn scaled_pixels(logical_size: f64, scale: u8) -> u32 {
    (logical_size * f64::from(scale)).round().max(1.0) as u32
}

fn render_ios_variant(source: &[u8], width: u32, height: u32) -> Result<Vec<u8>> {
    let image = image::load_from_memory(source)?;
    let rendered = image.resize_exact(width, height, image::imageops::FilterType::Lanczos3);
    let mut output = Cursor::new(Vec::new());
    rendered.write_to(&mut output, image::ImageFormat::Png)?;
    Ok(output.into_inner())
}

fn collect_assets(
    value: &Value,
    mode: AssetMode,
    result: &mut Vec<Asset>,
    seen: &mut HashSet<String>,
) {
    let Some(object) = value.as_object() else {
        if let Some(array) = value.as_array() {
            for item in array {
                collect_assets(item, mode, result, seen);
            }
        };
        return;
    };
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("asset");
    let layer_type = object.get("type").and_then(Value::as_str).unwrap_or("");
    let exportable = object
        .get("exportable")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let width = object.get("width").and_then(Value::as_f64);
    let height = object.get("height").and_then(Value::as_f64);
    let x = object.get("left").and_then(Value::as_f64);
    let y = object.get("top").and_then(Value::as_f64);
    let explicit_image = object.get("image").and_then(Value::as_object);
    if mode == AssetMode::Cutouts && exportable {
        if let Some(url) = explicit_image
            .and_then(|image| image.get("imageUrl"))
            .and_then(Value::as_str)
        {
            add_asset(result, seen, name, url, "png", width, height, x, y);
        }
    } else if mode != AssetMode::Cutouts {
        if let Some(url) = explicit_image
            .and_then(|image| image.get("svgUrl"))
            .and_then(Value::as_str)
        {
            add_asset(result, seen, name, url, "svg", width, height, x, y);
        }
        if let Some(url) = explicit_image
            .and_then(|image| image.get("imageUrl"))
            .and_then(Value::as_str)
        {
            add_asset(result, seen, name, url, "png", width, height, x, y);
        }
    }
    if mode == AssetMode::FullLayers
        && layer_type != "text"
        && let Some(url) = object
            .get("ddsImage")
            .and_then(|image| image.get("imageUrl"))
            .and_then(Value::as_str)
    {
        add_asset(result, seen, name, url, "dds-png", width, height, x, y);
    }
    for child in object.values() {
        collect_assets(child, mode, result, seen);
    }
}

#[allow(clippy::too_many_arguments)]
fn add_asset(
    result: &mut Vec<Asset>,
    seen: &mut HashSet<String>,
    name: &str,
    url: &str,
    kind: &'static str,
    width: Option<f64>,
    height: Option<f64>,
    x: Option<f64>,
    y: Option<f64>,
) {
    if seen.insert(url.to_owned()) {
        result.push(Asset {
            name: name.to_owned(),
            url: url.to_owned(),
            kind,
            width,
            height,
            x,
            y,
        });
    }
}

async fn get_json(client: &Client, url: Url, headers: &HeaderMap) -> Result<Value> {
    Ok(client
        .get(url)
        .headers(headers.clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}
async fn get_bytes(client: &Client, url: Url, headers: &HeaderMap) -> Result<Download> {
    let response = client
        .get(url)
        .headers(headers.clone())
        .send()
        .await?
        .error_for_status()?;
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    Ok(Download {
        body: response.bytes().await?.to_vec(),
        content_type,
    })
}
fn query_value(url: &Url, name: &str) -> Result<String> {
    url.query_pairs()
        .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
        .context(format!("list URL has no {name}"))
}
fn checked_url(raw: &str) -> Result<Url> {
    let url = Url::parse(raw)?;
    if !is_lanhu_host(&url) {
        bail!(
            "refusing to send credentials to non-Lanhu URL: {}",
            url.host_str().unwrap_or("unknown")
        );
    }
    Ok(url)
}
fn safe_name(value: &str) -> String {
    let value: String = value
        .chars()
        .map(|char| {
            if char.is_alphanumeric() || matches!(char, '-' | '_' | ' ') {
                char
            } else {
                '_'
            }
        })
        .collect();
    let value = value
        .trim()
        .replace(' ', "-")
        .chars()
        .take(80)
        .collect::<String>()
        .trim_matches('-')
        .to_owned();
    if value.is_empty() {
        "untitled".to_owned()
    } else {
        value
    }
}
fn short_id(value: &str) -> &str {
    value.get(..8).unwrap_or(value)
}
fn extension_for(kind: &str, content_type: &Option<String>) -> &'static str {
    if kind == "svg"
        || content_type
            .as_deref()
            .is_some_and(|value| value.contains("svg"))
    {
        "svg"
    } else if content_type
        .as_deref()
        .is_some_and(|value| value.contains("webp"))
    {
        "webp"
    } else if content_type
        .as_deref()
        .is_some_and(|value| value.contains("jpeg"))
    {
        "jpg"
    } else {
        "png"
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::io::Cursor;

    use image::{DynamicImage, GenericImageView, ImageBuffer, Rgba};
    use serde_json::json;

    use super::{
        AssetMode, ExportManifest, PageManifest, collect_assets, render_ai_handoff,
        render_ios_variant, safe_name, scaled_pixels,
    };

    #[test]
    fn reconstruction_assets_include_svg_but_not_dds() {
        let layer = json!({"name":"icon", "type":"symbol", "image":{"svgUrl":"https://lanhuapp.com/icon.svg"}, "ddsImage":{"imageUrl":"https://lanhuapp.com/icon.png"}});
        let mut assets = Vec::new();
        collect_assets(&layer, AssetMode::Smart, &mut assets, &mut HashSet::new());
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].kind, "svg");
    }

    #[test]
    fn lanhu_cutouts_exclude_non_exportable_and_svg_assets() {
        let layer = json!({"name":"photo", "type":"shape", "image":{"imageUrl":"https://lanhuapp.com/photo.png"}});
        let mut assets = Vec::new();
        collect_assets(&layer, AssetMode::Cutouts, &mut assets, &mut HashSet::new());
        assert!(assets.is_empty());
    }

    #[test]
    fn lanhu_cutouts_include_only_exportable_png_assets() {
        let layer = json!({"name":"icon", "exportable":true, "image":{"svgUrl":"https://lanhuapp.com/icon.svg", "imageUrl":"https://lanhuapp.com/icon.png"}});
        let mut assets = Vec::new();
        collect_assets(&layer, AssetMode::Cutouts, &mut assets, &mut HashSet::new());
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].kind, "png");
    }

    #[test]
    fn full_layers_include_non_exportable_dds_images() {
        let layer = json!({"name":"system", "type":"layer-group", "exportable":false, "ddsImage":{"imageUrl":"https://lanhuapp.com/system.png"}});
        let mut assets = Vec::new();
        collect_assets(
            &layer,
            AssetMode::FullLayers,
            &mut assets,
            &mut HashSet::new(),
        );
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].kind, "dds-png");
    }

    #[test]
    fn renders_exact_ios_png_dimensions() {
        let source =
            DynamicImage::ImageRgba8(ImageBuffer::from_pixel(16, 16, Rgba([0, 255, 0, 255])));
        let mut png = Cursor::new(Vec::new());
        source.write_to(&mut png, image::ImageFormat::Png).unwrap();
        let rendered = render_ios_variant(&png.into_inner(), 12, 18).unwrap();
        assert_eq!(
            image::load_from_memory(&rendered).unwrap().dimensions(),
            (12, 18)
        );
        assert_eq!(scaled_pixels(28.5, 2), 57);
    }

    #[test]
    fn handoff_template_includes_export_metadata() {
        let manifest = ExportManifest {
            project: "Example project".to_owned(),
            pages: vec![PageManifest {
                image_id: "page-id".to_owned(),
                name: "Example page".to_owned(),
                versions: Vec::new(),
            }],
        };
        let handoff = render_ai_handoff(&manifest, AssetMode::Cutouts);
        assert!(handoff.contains("Project: Example project"));
        assert!(handoff.contains("contains 1 pages"));
        assert!(handoff.contains("Lanhu cutout mode"));
    }

    #[test]
    fn produces_a_fallback_safe_file_name() {
        assert_eq!(safe_name("///"), "___");
        assert_eq!(safe_name("   "), "untitled");
    }
}
