mod curl;
mod exporter;
mod model;

use std::io::{self, stdout};
use std::path::Path;
use std::process::Command;
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use crossterm::cursor::MoveTo;
use crossterm::event::{
    self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEventKind, KeyModifiers,
};
use crossterm::execute;
use crossterm::terminal::{
    Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::prelude::*;
use ratatui::widgets::*;

use crate::model::{AssetMode, ExportOptions, ProgressEvent, VersionMode, default_concurrency};

struct App {
    curl_input: String,
    focus: usize,
    asset_mode: usize,
    version_mode: usize,
    status: String,
    progress: (usize, usize),
    exporting: bool,
    should_quit: bool,
    receiver: Option<Receiver<ProgressEvent>>,
}

impl Default for App {
    fn default() -> Self {
        Self {
            curl_input: String::new(),
            focus: 0,
            asset_mode: 0,
            version_mode: 0,
            status: "等待粘贴蓝湖 images curl".into(),
            progress: (0, 0),
            exporting: false,
            should_quit: false,
            receiver: None,
        }
    }
}

fn main() -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut out = stdout();
    execute!(
        out,
        EnterAlternateScreen,
        EnableBracketedPaste,
        Clear(ClearType::All),
        MoveTo(0, 0)
    )?;
    let mut terminal = Terminal::new(CrosstermBackend::new(out))?;
    let result = run(&mut terminal);
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        DisableBracketedPaste,
        LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;
    result
}

fn run(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> anyhow::Result<()> {
    let mut app = App::default();
    while !app.should_quit {
        terminal.draw(|frame| draw(frame, &app))?;
        receive_progress(&mut app);
        if event::poll(Duration::from_millis(100))? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => {
                    handle_key(&mut app, key.code, key.modifiers)
                }
                Event::Paste(text) if app.focus == 0 && !app.exporting => {
                    app.curl_input.push_str(&text)
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn receive_progress(app: &mut App) {
    let mut finished = false;
    if let Some(receiver) = &app.receiver {
        while let Ok(event) = receiver.try_recv() {
            match event {
                ProgressEvent::Started { project, pages } => {
                    app.status = format!("Exporting {project} ({pages} pages)");
                    app.progress = (0, pages);
                }
                ProgressEvent::Status(text) => app.status = text,
                ProgressEvent::Progress { done, total } => app.progress = (done, total),
                ProgressEvent::Finished { output, failures } => {
                    app.status = match open_output_directory(&output) {
                        Ok(()) => format!("导出完成：{failures} 项失败，已在 Finder 中打开目录"),
                        Err(error) => format!("导出完成：{failures} 项失败，无法打开目录：{error}"),
                    };
                    finished = true;
                }
                ProgressEvent::Failed(error) => {
                    app.status = format!("Export failed: {error}");
                    finished = true;
                }
            }
        }
    }
    if finished {
        app.exporting = false;
        app.receiver = None;
    }
}

fn open_output_directory(path: &str) -> io::Result<()> {
    Command::new("open")
        .arg(Path::new(path))
        .spawn()
        .map(|_| ())
}

fn handle_key(app: &mut App, code: KeyCode, modifiers: KeyModifiers) {
    match code {
        KeyCode::Esc if !app.exporting => app.should_quit = true,
        KeyCode::Char('q') if app.focus != 0 && !app.exporting => app.should_quit = true,
        KeyCode::Tab if !app.exporting => app.focus = (app.focus + 1) % 3,
        KeyCode::BackTab if !app.exporting => app.focus = (app.focus + 2) % 3,
        KeyCode::Backspace if app.focus == 0 && !app.exporting => {
            app.curl_input.pop();
        }
        KeyCode::Char('u')
            if app.focus == 0 && modifiers.contains(KeyModifiers::CONTROL) && !app.exporting =>
        {
            app.curl_input.clear();
        }
        KeyCode::Char(char) if app.focus == 0 && !app.exporting => app.curl_input.push(char),
        KeyCode::Left | KeyCode::Right if app.focus == 1 && !app.exporting => {
            app.asset_mode = (app.asset_mode + 1) % AssetMode::ALL.len()
        }
        KeyCode::Left | KeyCode::Right if app.focus == 2 && !app.exporting => {
            app.version_mode = (app.version_mode + 1) % VersionMode::ALL.len()
        }
        KeyCode::Enter if !app.exporting => start_export(app),
        _ => {}
    }
}

fn start_export(app: &mut App) {
    let request = match curl::parse_list_curl(&app.curl_input) {
        Ok(request) => request,
        Err(error) => {
            app.status = format!("Invalid curl: {error:#}");
            return;
        }
    };
    let options = ExportOptions {
        asset_mode: AssetMode::ALL[app.asset_mode],
        version_mode: VersionMode::ALL[app.version_mode],
        concurrency: default_concurrency(),
    };
    let (tx, rx) = mpsc::channel();
    app.receiver = Some(rx);
    app.exporting = true;
    app.status = "Starting secure export...".into();
    std::thread::spawn(move || {
        let result = match tokio::runtime::Runtime::new() {
            Ok(runtime) => runtime.block_on(exporter::run(request, options, tx.clone())),
            Err(error) => Err(error.into()),
        };
        if let Err(error) = result {
            tx.send(ProgressEvent::Failed(format!("{error:#}"))).ok();
        }
    });
}

fn draw(frame: &mut Frame, app: &App) {
    let outer = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .margin(2)
    .split(frame.area());
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                " LANHU ",
                Style::new().fg(Color::Black).bg(Color::Cyan).bold(),
            ),
            Span::styled(" Asset Exporter", Style::new().fg(Color::Cyan).bold()),
            Span::raw("  Sketch JSON + 切图"),
            Span::styled(
                format!("  CPU 并发 {}", default_concurrency()),
                Style::new().fg(Color::DarkGray),
            ),
        ]))
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::new().fg(Color::DarkGray)),
        ),
        outer[0],
    );
    let input_style = if app.focus == 0 {
        Style::new().fg(Color::Cyan)
    } else {
        Style::new().fg(Color::Gray)
    };
    let curl_content = if app.curl_input.is_empty() {
        "在此粘贴 /api/project/images 的完整 curl 请求".to_owned()
    } else {
        format!("已粘贴 {} 个字符", app.curl_input.chars().count())
    };
    frame.render_widget(
        Paragraph::new(curl_content)
            .style(input_style)
            .alignment(Alignment::Center)
            .block(
                Block::bordered()
                    .title(" 1  粘贴 images curl ")
                    .border_style(focus_style(app.focus == 0)),
            ),
        outer[1],
    );
    let controls = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(outer[2]);
    frame.render_widget(
        Paragraph::new(format!(
            "{}  ·  {}",
            AssetMode::ALL[app.asset_mode].label(),
            AssetMode::ALL[app.asset_mode].description()
        ))
        .alignment(Alignment::Center)
        .block(
            Block::bordered()
                .title(" 2  素材模式 ")
                .border_style(focus_style(app.focus == 1)),
        ),
        controls[0],
    );
    frame.render_widget(
        Paragraph::new(VersionMode::ALL[app.version_mode].label()).block(
            Block::bordered()
                .title(" 3  导出版本 ")
                .border_style(focus_style(app.focus == 2)),
        ),
        controls[1],
    );
    let action = if app.exporting {
        " 正在导出，请等待完成 ".to_owned()
    } else {
        format!(" ENTER  开始导出  ·  并发 {} ", default_concurrency())
    };
    let action_style = Style::new().bg(if app.exporting {
        Color::Yellow
    } else {
        Color::Cyan
    });
    frame.render_widget(Block::default().style(action_style), outer[3]);
    frame.render_widget(
        Paragraph::new(action)
            .alignment(Alignment::Center)
            .style(Style::new().fg(Color::Black).bold()),
        outer[3],
    );
    let ratio = if app.progress.1 == 0 {
        0.0
    } else {
        app.progress.0 as f64 / app.progress.1 as f64
    };
    frame.render_widget(
        Gauge::default()
            .block(
                Block::bordered()
                    .title(" 导出进度 ")
                    .border_style(Style::new().fg(Color::DarkGray)),
            )
            .gauge_style(Style::new().fg(Color::Cyan))
            .ratio(ratio)
            .label(app.status.as_str()),
        outer[4],
    );
    let hint = if app.exporting {
        "导出时暂时锁定输入，完成后可继续操作"
    } else {
        "Tab 切换  |  ← → 选择  |  Ctrl+U 清空  |  Esc 退出"
    };
    frame.render_widget(
        Paragraph::new(hint)
            .alignment(Alignment::Center)
            .style(Style::new().fg(Color::DarkGray)),
        outer[5],
    );
}

fn focus_style(focused: bool) -> Style {
    if focused {
        Style::new().fg(Color::Cyan).bold()
    } else {
        Style::new().fg(Color::DarkGray)
    }
}
