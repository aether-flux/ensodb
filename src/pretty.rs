use crate::types::{TableSchema, Value};

fn separator(widths: &[usize]) -> String {
    widths
        .iter()
        .map(|w| format!("+{}+", "-".repeat(*w + 2)))
        .collect::<Vec<_>>()
        .join("")
}

pub fn pretty_rows(schema: &TableSchema, rows: &Vec<Vec<Value>>) -> String {
    let mut widths = vec![0; schema.columns.len()];

    // column names
    for (i, col) in schema.columns.iter().enumerate() {
        widths[i] = col.name.len();
    }

    // row values
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            widths[i] = widths[i].max(val.to_string().len());
        }
    }

    let mut out = String::new();

    // top separator
    out.push_str(&separator(&widths));
    out.push('\n');

    // header
    let header = schema.columns.iter().enumerate()
        .map(|(i, col)| format!("| {:width$} ", col.name, width = widths[i]))
        .collect::<Vec<_>>()
        .join("");

    out.push_str(&header);
    out.push_str("|\n");

    // header separator
    out.push_str(&separator(&widths));
    out.push('\n');

    // rows
    for row in rows {
        let line = row.iter().enumerate()
            .map(|(i, val)| format!("| {:width$} ", val, width = widths[i]))
            .collect::<Vec<_>>()
            .join("");

        out.push_str(&line);
        out.push_str("|\n");
    }

    // bottom separator
    out.push_str(&separator(&widths));
    out.push('\n');

    // row count
    out.push_str(format!("({} rows)", rows.len()).as_str());

    out
}
