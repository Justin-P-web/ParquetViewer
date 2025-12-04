use std::fs::File;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use arrow::util::display::array_value_to_string;
use clap::Parser;
use gpui::{
    div, prelude::*, px, rgb, size, App, Application, Bounds, MouseButton, WindowBounds,
    WindowOptions,
};
use gpui_component::{scroll::ScrollableElement, StyledExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;
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
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    row_count: usize,
    column_count: usize,
}

fn main() -> Result<(), ViewerError> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    info!(
        path = %args.path.to_string_lossy(),
        rows = args.rows,
        "loading parquet file"
    );
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
    let metadata = SerializedFileReader::new(file.try_clone()?)?
        .metadata()
        .clone();
    let parquet_schema = format!(
        "{:#?}",
        metadata.file_metadata().schema_descr().root_schema()
    );
    let row_count = metadata.file_metadata().num_rows() as usize;
    let column_count = metadata.file_metadata().schema_descr().columns().len();

    // Use the Arrow reader to fetch record batches.
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(row_limit);
    let mut batch_reader = reader.build()?;
    let mut batches: Vec<RecordBatch> = Vec::new();

    while let Some(batch) = batch_reader.next() {
        batches.push(batch?);
        if batches.iter().map(|b| b.num_rows()).sum::<usize>() >= row_limit {
            break;
        }
    }

    let columns = if let Some(batch) = batches.first() {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    } else {
        Vec::new()
    };

    let mut rows = Vec::new();
    for batch in &batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::new();
            for column_index in 0..batch.num_columns() {
                let column = batch.column(column_index);
                let value = if column.is_null(row_index) {
                    "null".to_string()
                } else {
                    array_value_to_string(column.as_ref(), row_index)?
                };
                row.push(value);
            }
            rows.push(row);

            if rows.len() >= row_limit {
                break;
            }
        }

        if rows.len() >= row_limit {
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
        columns,
        rows,
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
    let preview_data = preview.clone();

    Application::new().run(move |app: &mut App| {
        gpui_component::init(app);

        let bounds = Bounds::centered(None, size(px(900.0), px(700.0)), app);
                app.open_window(
                    WindowOptions {
                        window_bounds: Some(WindowBounds::Windowed(bounds)),
                        titlebar: Some(gpui::TitlebarOptions {
                            title: Some("Parquet Viewer".into()),
                    ..Default::default()
                }),
                ..Default::default()
                    },
                    move |_window, cx| {
                        cx.new(|_| PreviewView {
                            preview: preview_data.clone(),
                            selected_cell: None,
                        })
                    },
                )
                .unwrap();
        app.activate(true);
    });
}

struct PreviewView {
    preview: DataPreview,
    selected_cell: Option<(usize, usize)>,
}

impl gpui::Render for PreviewView {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        cx: &mut gpui::Context<Self>,
    ) -> impl gpui::IntoElement {
        let metadata = format!(
            "Rows: {} | Columns: {}",
            self.preview.row_count, self.preview.column_count
        );

        let selected_text = self
            .selected_cell
            .map(|(row, col)| format!("Selected: row {}, column {}", row + 1, col + 1))
            .unwrap_or_else(|| "Click a cell to select it".to_string());

        div()
            .flex()
            .flex_col()
            .gap_3()
            .p_4()
            .size_full()
            .child(div().text_xl().child("Parquet Overview"))
            .child(
                div()
                    .flex_col()
                    .gap_2()
                    .w_full()
                    .child(div().font_family("monospace").child(format!(
                        "Schema\n{}",
                        self.preview.schema
                    )))
                    .child(div().font_medium().child(metadata))
                    .child(div().text_sm().child(selected_text))
                    .child(render_table(&self.preview, self.selected_cell, cx)),
            )
    }
}

fn render_table(
    preview: &DataPreview,
    selected_cell: Option<(usize, usize)>,
    cx: &mut gpui::Context<PreviewView>,
) -> impl gpui::IntoElement {
    let header = div()
        .flex()
        .flex_row()
        .bg(rgb(0xF8FAFC))
        .border_b_1()
        .border_color(rgb(0xE5E7EB))
        .children(preview.columns.iter().map(|name| {
            div()
                .px_2()
                .py_1()
                .font_medium()
                .border_r_1()
                .border_color(rgb(0xE5E7EB))
                .child(name.clone())
        }));

    let rows = preview.rows.iter().enumerate().map(|(row_index, row)| {
        div()
            .flex()
            .flex_row()
            .border_b_1()
            .border_color(rgb(0xE5E7EB))
            .children(row.iter().enumerate().map(|(col_index, value)| {
                let is_selected = selected_cell == Some((row_index, col_index));
                let click_handler = cx.listener(move |view: &mut PreviewView, _: &gpui::MouseDownEvent, _window, _cx| {
                    view.selected_cell = Some((row_index, col_index));
                });

                let background = if is_selected {
                    rgb(0xDBEAFE)
                } else if row_index % 2 == 0 {
                    rgb(0xFFFFFF)
                } else {
                    rgb(0xF8FAFC)
                };

                div()
                    .px_2()
                    .py_1()
                    .min_w(px(80.0))
                    .border_r_1()
                    .border_color(rgb(0xE5E7EB))
                    .bg(background)
                    .hover(|this| this.bg(rgb(0xE0F2FE)))
                    .cursor_pointer()
                    .on_mouse_down(MouseButton::Left, click_handler)
                    .child(value.clone())
            }))
    });

    div()
        .border_1()
        .border_color(rgb(0xE5E7EB))
        .rounded_md()
        .h(px(400.0))
        .overflow_scrollbar()
        .child(div().flex().flex_col().font_family("monospace").child(header).children(rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::NamedTempFile;

    fn write_test_parquet(rows: usize) -> Result<NamedTempFile, ViewerError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids: Vec<i32> = (0..rows as i32).collect();
        let names: Vec<String> = ids.iter().map(|v| format!("name-{v}")).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )?;

        let file = NamedTempFile::new()?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file.reopen()?, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(file)
    }

    #[test]
    fn load_preview_reports_metadata() {
        let file = write_test_parquet(4).expect("parquet write should succeed");

        let preview = load_preview(&file.path().to_path_buf(), 10).expect("preview should load");

        assert_eq!(preview.row_count, 4);
        assert_eq!(preview.column_count, 2);
        assert!(preview.formatted_rows.contains("id"));
        assert!(preview.formatted_rows.contains("name-0"));
        assert_eq!(preview.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(preview.rows.len(), 4);
        assert_eq!(preview.rows[0], vec!["0".to_string(), "name-0".to_string()]);
    }

    #[test]
    fn load_preview_respects_row_limit() {
        let file = write_test_parquet(5).expect("parquet write should succeed");

        let preview = load_preview(&file.path().to_path_buf(), 2).expect("preview should load");

        assert!(preview.formatted_rows.contains("name-0"));
        assert!(preview.formatted_rows.contains("name-1"));
        assert!(!preview.formatted_rows.contains("name-2"));
        assert_eq!(preview.rows.len(), 2);
    }
}
