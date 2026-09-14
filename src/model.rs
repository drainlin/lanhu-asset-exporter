use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetMode {
    Smart,
    Cutouts,
    FullLayers,
}

impl AssetMode {
    pub const ALL: [Self; 3] = [Self::Cutouts, Self::Smart, Self::FullLayers];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Cutouts => "蓝湖切图",
            Self::Smart => "页面还原素材",
            Self::FullLayers => "全量图层",
        }
    }

    pub const fn description(self) -> &'static str {
        match self {
            Self::Cutouts => "仅 exportable PNG",
            Self::Smart => "所有显式 SVG/PNG",
            Self::FullLayers => "显式素材与 DDS 渲染",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionMode {
    Latest,
    All,
}

impl VersionMode {
    pub const ALL: [Self; 2] = [Self::Latest, Self::All];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Latest => "最新版本",
            Self::All => "全部版本",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExportOptions {
    pub asset_mode: AssetMode,
    pub version_mode: VersionMode,
    pub concurrency: usize,
}

pub fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
        .min(6)
}

#[cfg(test)]
mod tests {
    use super::default_concurrency;

    #[test]
    fn concurrency_is_always_between_one_and_six() {
        assert!((1..=6).contains(&default_concurrency()));
    }
}

#[derive(Debug, Serialize)]
pub struct ExportManifest {
    pub project: String,
    pub pages: Vec<PageManifest>,
}

#[derive(Debug, Serialize)]
pub struct PageManifest {
    pub image_id: String,
    pub name: String,
    pub versions: Vec<VersionManifest>,
}

#[derive(Debug, Serialize)]
pub struct VersionManifest {
    pub version_id: String,
    pub sketch_json: String,
    pub preview: Option<String>,
    pub assets: Vec<AssetManifest>,
}

#[derive(Debug, Serialize)]
pub struct AssetManifest {
    pub layer_name: String,
    pub kind: String,
    pub local_path: String,
    pub variants: Vec<AssetVariantManifest>,
    pub width: Option<f64>,
    pub height: Option<f64>,
    pub x: Option<f64>,
    pub y: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct AssetVariantManifest {
    pub scale: u8,
    pub local_path: String,
    pub pixel_width: u32,
    pub pixel_height: u32,
}

#[derive(Debug)]
pub enum ProgressEvent {
    Started { project: String, pages: usize },
    Status(String),
    Progress { done: usize, total: usize },
    Finished { output: String, failures: usize },
    Failed(String),
}
