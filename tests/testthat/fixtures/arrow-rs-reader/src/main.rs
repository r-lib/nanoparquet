// Reads a Parquet file via the Arrow interface. This triggers the flatbuffer
// verifier on the ARROW:schema metadata key, which rejects misaligned int64_t
// vectors (see https://github.com/r-lib/nanoparquet/issues/152).
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: arrow-rs-reader <file.parquet>");
    let file = File::open(&path).expect("cannot open file");
    ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("failed to read Arrow schema from parquet file");
    println!("OK");
}
