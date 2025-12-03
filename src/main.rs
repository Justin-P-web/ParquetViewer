use std::fs::File;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::SerializedFileReader;
use parquet::file::serialized_reader::SliceableCursor;
use thiserror::Error;
use tracing::info;

/// Command line arguments for the viewer.
#[derive(Parser, Debug)]
#[command(
    name = "parquet-viewer",
    about = "Inspect Parquet files with a GPUI front-end"
)]
struct Args {
    /// Path to the Parquet file.
    #[arg(value_name = "FILE")]
    path: PathBuf,

    /// Number of rows to preview from the top of the file.
    #[arg(short, long, default_value_t = 20)]
    rows: usize,

    /// Render the preview to stdout instead of launching the UI.
    #[arg(long, default_value_t = false)]
    headless: bool,
}

#[derive(Debug, Error)]
enum ViewerError {
    #[error("failed to open parquet file: {0}")]
    OpenFailed(#[from] std::io::Error),

    #[error("failed to read parquet batches: {0}")]
    ReadFailed(#[from] parquet::errors::ParquetError),

    #[error("failed to format parquet preview: {0}")]
    FormatFailed(#[from] arrow::error::ArrowError),
}

#[derive(Clone)]
struct DataPreview {
    schema: String,
    formatted_rows: String,
    row_count: usize,
    column_count: usize,
}

fn main() -> Result<(), ViewerError> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    info!("loading parquet file", path = ?args.path, rows = args.rows);
    let preview = load_preview(&args.path, args.rows)?;

    if args.headless {
        print_to_terminal(&preview);
        return Ok(());
    }

    launch_ui(preview);

    Ok(())
}

fn load_preview(path: &PathBuf, row_limit: usize) -> Result<DataPreview, ViewerError> {
    let file = File::open(path)?;
    let mut cursor = SliceableCursor::new(file);

    // Pull metadata for quick summary stats.
    let metadata = SerializedFileReader::new(cursor.clone())?
        .metadata()
        .clone();
    let parquet_schema = metadata
        .file_metadata()
        .schema_descr()
        .root_schema()
        .to_string();
    let row_count = metadata.file_metadata().num_rows() as usize;
    let column_count = metadata.file_metadata().schema_descr().columns().len();

    // Use the Arrow reader to fetch record batches.
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(cursor)?.with_batch_size(row_limit);
    let mut batch_reader = reader.build()?;
    let mut batches: Vec<RecordBatch> = Vec::new();

    while let Some(batch) = batch_reader.next() {
        batches.push(batch?);
        if batches.iter().map(|b| b.num_rows()).sum::<usize>() >= row_limit {
            break;
        }
    }

    let formatted_rows = if batches.is_empty() {
        "(no rows found)".to_string()
    } else {
        pretty_format_batches(&batches)?.to_string()
    };

    Ok(DataPreview {
        schema: parquet_schema,
        formatted_rows,
        row_count,
        column_count,
    })
}

fn print_to_terminal(preview: &DataPreview) {
    println!("Schema:\n{}", preview.schema);
    println!(
        "Rows: {} | Columns: {}\n",
        preview.row_count, preview.column_count
    );
    println!("{}", preview.formatted_rows);
}

/// Launch a GPUI window that renders the formatted preview.
fn launch_ui(preview: DataPreview) {
    // The gpui runtime owns its own executor; we simply feed the precomputed strings
    // into a lightweight view that shows schema details and a pretty-printed table.
    gpui::run_app(|app| {
        app.open_window(
            gpui::WindowOptions {
                titlebar: Some(gpui::TitlebarOptions {
                    title: Some("Parquet Viewer".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            move |cx| {
                cx.new_view(|_cx| PreviewView {
                    preview: preview.clone(),
                })
            },
        );
    });
}

struct PreviewView {
    preview: DataPreview,
}

impl gpui::View for PreviewView {
    fn render(&mut self, _cx: &mut gpui::ViewContext<Self>) -> impl gpui::IntoElement {
        let mut text = String::new();
        text.push_str("Schema\n");
        text.push_str(&self.preview.schema);
        text.push_str("\n\n");
        text.push_str(&format!(
            "Rows: {} | Columns: {}\n\n",
            self.preview.row_count, self.preview.column_count
        ));
        text.push_str(&self.preview.formatted_rows);

        gpui::prelude::vstack()
            .gap(gpui::prelude::px(12.0))
            .push(gpui::prelude::label("Parquet Overview").size(18.0))
            .push(
                gpui::prelude::scroll().with(
                    gpui::prelude::div()
                        .text(text)
                        .font_family(gpui::prelude::FontFamily::Monospace),
                ),
            )
    }
}
