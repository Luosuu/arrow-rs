use arrow::util::pretty::print_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::Result;
use parquet::file::direct_page::get_page_by_idx;
use std::fs::File;
use std::sync::Arc;
use arrow::datatypes::DataType;
use parquet::arrow::array_reader::byte_array::{ByteArrayColumnValueDecoder, ByteArrayDecoder};
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::reader::{ChunkReader, FileReader};
use parquet::file::reader::SerializedFileReader;
use parquet::schema::types::{ColumnDescPtr, SchemaDescriptor};

fn main() -> Result<()> {
    // Create parquet file that will be read.
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
    let test_file = File::open(path).unwrap();

    let row_group_idx = 0;
    let column_idx = 0;
    let page_idx = 0;

    let test_file_reader = SerializedFileReader::new(test_file.try_clone().unwrap()).unwrap();
    let parquet_metadata = test_file_reader.metadata();

    // Get the column descriptor for the desired column (assuming column index 0)
    let column_desc: ColumnDescPtr = Arc::new(parquet_metadata.file_metadata().schema().column(0).clone());

    // Get the page for the desired column chunk and page index
    let page = get_page_by_idx(test_file, 0, 0, 0).unwrap().unwrap();
    let bytes = page.buffer().get_bytes(0, page.buffer().len()).unwrap();

    // Create a ByteArrayColumnValueDecoder for the column
    let mut decoder = ByteArrayColumnValueDecoder::new(&column_desc);
    decoder.set_data(page.encoding(), bytes, page.num_values() as usize, Some(page.num_values() as usize)).unwrap();

    // Read the values into an OffsetBuffer
    // let mut buffer = parquet::arrow::record_reader::buffer::OffsetBuffer::<i32>::default();
    let mut buffer = Vec::new();
    let num_values = decoder.read(&mut buffer, page.num_values() as usize).unwrap();

    // Convert the Vec<u8> to an Arrow Array
    //  let array = buffer.into_array(None, DataType::Binary);
    let array = arrow::array::BinaryArray::from_vec(buffer);

    // Create a SchemaDescriptor from the Parquet schema
    let parquet_schema = parquet_metadata.file_metadata().schema_descr().clone();

    // Create a RecordBatch from the array
    // Create a RecordBatch from the array
    let schema = parquet_to_arrow_schema(
        &parquet_schema,
        parquet_metadata.file_metadata().key_value_metadata(),
    )
        .unwrap();
    let batch = arrow::record_batch::RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)]).unwrap();


    print_batches(&[batch]).unwrap();

    Ok(())
}