use arrow_array::types::Utf8Type;
use arrow_array::{Array, GenericByteArray, PrimitiveArray, RecordBatch};
use arrow_schema::DataType;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use std::sync::Arc;
use std::{fs::File, io::Read};

use crate::arrow::array_reader::byte_array::ByteArrayColumnValueDecoder;
use crate::arrow::parquet_to_arrow_schema;
use crate::arrow::record_reader::{GenericRecordReader, RecordReader};
use crate::column::page::{Page, PageReader};
use crate::column::reader::{ColumnReader, get_column_reader, get_typed_column_reader};
use crate::column::reader::decoder::{ColumnValueDecoder, ColumnValueDecoderImpl};
use crate::compression::{create_codec, Codec};
use crate::basic::Type as PhysicalType;
use crate::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::metadata::thrift::PageHeader;
use crate::file::page_index::index_reader::read_offset_indexes;
use crate::file::page_index::offset_index::PageLocation;
use crate::file::properties::ReaderProperties;
use crate::file::reader::*;
use crate::file::serialized_reader::decode_page;
use crate::parquet_thrift::{ReadThrift, ThriftSliceInputProtocol};
use crate::util::test_common::page_util::InMemoryPageReader;

pub fn get_file_page_locations(file: File) -> Result<Option<Vec<Vec<Vec<PageLocation>>>>> {
    let file_clone = file.try_clone().unwrap();
    let file_reader = SerializedFileReader::new(file_clone).unwrap();
    let num_row_groups = file_reader.num_row_groups();
    let mut file_page_locations = Vec::new();
    #[allow(deprecated)]
    for row_group_idx in 0..num_row_groups {
        let row_group_reader = file_reader.get_row_group(row_group_idx)?;
        let offset_indexes = read_offset_indexes(&file, row_group_reader.metadata().columns())?;
        // Convert from Vec<OffsetIndexMetaData> to Vec<Vec<PageLocation>>
        let page_locations = offset_indexes.map(|indexes| {
            indexes
                .into_iter()
                .map(|idx| idx.page_locations().clone())
                .collect()
        });
        file_page_locations.push(page_locations.unwrap_or_default())
    }

    Ok(Some(file_page_locations)) // 3 dim, row_group x column x page
}

pub fn generate_random_page_indices_file_level(
    file_page_locations: Vec<Vec<Vec<PageLocation>>>,
    column_idx: usize,
) -> Result<Vec<(usize, usize)>> {
    let num_row_groups = file_page_locations.len();
    let mut page_num_offsets = Vec::new();
    let mut total_page_num = 0;

    for row_group_idx in 0..num_row_groups {
        page_num_offsets.push(total_page_num);
        total_page_num += file_page_locations[row_group_idx][column_idx].len();
    }

    let mut page_random_indices: Vec<usize> = (0..total_page_num).collect();
    page_random_indices.shuffle(&mut thread_rng());

    // binary search into the page_num_offsets to create (row_group_idx, page_idx) pairs
    let mut random_rg_page_indices_pairs = Vec::new();
    for &page_random_index in &page_random_indices {
        let row_group_idx = page_num_offsets
            .binary_search(&page_random_index)
            .unwrap_or_else(|idx| idx - 1);
        let page_idx = page_random_index - page_num_offsets[row_group_idx];
        random_rg_page_indices_pairs.push((row_group_idx, page_idx));
    }
    Ok(random_rg_page_indices_pairs) // 2-dim, (row_group_idx, page_idx)
}

pub fn generate_random_page_indices_dataset_level(
    dataset_page_locations: Vec<Vec<Vec<Vec<PageLocation>>>>,
    column_idx: usize,
) -> Result<Vec<(usize, usize, usize)>> {
    let num_files = dataset_page_locations.len();
    let mut page_num_offsets_across_file = Vec::new();
    let mut page_num_offsets_within_file = Vec::new();
    let mut total_page_num = 0;
    for file_idx in 0..num_files {
        let this_file_num_row_groups = dataset_page_locations[file_idx].len();
        let mut this_file_page_num_offsets = Vec::new();
        let mut this_file_page_num = 0;

        for row_group_idx in 0..this_file_num_row_groups {
            this_file_page_num_offsets.push(this_file_page_num);
            this_file_page_num += dataset_page_locations[file_idx][row_group_idx][column_idx].len();
        }

        page_num_offsets_within_file.push(this_file_page_num_offsets);
        page_num_offsets_across_file.push(total_page_num);
        total_page_num += this_file_page_num;
    }

    // Generate random page indices
    let mut page_random_indices: Vec<usize> = (0..total_page_num).collect();
    page_random_indices.shuffle(&mut thread_rng());

    // binary search into two-layer offsets (page_num_offsets_across/within_file) to create random (file, row_group, page) pairs.
    let mut random_page_indices = Vec::new();
    for &page_random_index in &page_random_indices {
        let file_idx = page_num_offsets_across_file
            .binary_search(&page_random_index)
            .unwrap_or_else(|idx| idx - 1);
        let page_offset_within_file = page_random_index - page_num_offsets_across_file[file_idx];
        let row_group_idx = page_num_offsets_within_file[file_idx]
            .binary_search(&page_offset_within_file)
            .unwrap_or_else(|idx| idx - 1);
        let page_idx =
            page_offset_within_file - page_num_offsets_within_file[file_idx][row_group_idx];
        random_page_indices.push((file_idx, row_group_idx, page_idx));
    }

    Ok(random_page_indices)
}

pub fn get_page_by_location(
    file: File,
    page_location: PageLocation,
    column_meta: &ColumnChunkMetaData,
) -> Result<Option<Page>> {
    // buffer
    let buffer = file
        .get_bytes(
            page_location.offset as u64,
            page_location.compressed_page_size as usize,
        )
        .unwrap();
    let mut prot = ThriftSliceInputProtocol::new(buffer.as_ref());
    let page_header = PageHeader::read_thrift(&mut prot).unwrap();
    let offset = buffer.len() - prot.as_slice().len();

    let bytes = buffer.slice(offset..);

    // let column_meta = row_group_reader.metadata().column(column_idx);
    let physical_type = column_meta.column_type();
    let props = Arc::new(ReaderProperties::builder().build());
    let decompressor = &mut create_codec(column_meta.compression(), props.codec_options())?;

    let page = decode_page(page_header, bytes, physical_type, decompressor.as_mut())?;

    Ok(Some(page))
}

pub fn get_page_by_idx(
    file: File,
    row_group_idx: usize,
    column_idx: usize,
    page_idx: usize,
) -> Result<Option<Page>> {
    let file_clone = file.try_clone().unwrap();
    let file_reader = SerializedFileReader::new(file_clone).unwrap();
    let row_group_reader = file_reader.get_row_group(row_group_idx)?;
    // the iterative page reader for one column
    // let _page_reader = row_group_reader.get_column_page_reader(column_idx).unwrap();

    // Get the page location for the specified column and page index
    #[allow(deprecated)]
    let offset_indexes = read_offset_indexes(&file, row_group_reader.metadata().columns())?;
    let page_locations = offset_indexes
        .as_ref()
        .unwrap();
    let page_location = &page_locations[column_idx].page_locations()[page_idx];
    // buffer
    let buffer = file
        .get_bytes(
            page_location.offset as u64,
            page_location.compressed_page_size as usize,
        )
        .unwrap();
    let mut prot = ThriftSliceInputProtocol::new(buffer.as_ref());
    PageHeader::read_thrift(&mut prot).unwrap();
    let offset = buffer.len() - prot.as_slice().len();

    buffer.slice(offset..);

    let column_meta = row_group_reader.metadata().column(column_idx);
    let page = get_page_by_location(file, page_location.clone(), column_meta);

    page
}

/// a failed attempt to read page into batches through ByteArray decoder.
/// maybe record_reader instead of low-level ByteArray decoder should be used here.
pub fn read_page_into_batch(
    file: File,
    row_group_idx: usize,
    column_idx: usize,
    page_idx: usize,
) -> Result<Option<RecordBatch>> {
    let file_reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
    let parquet_metadata = file_reader.metadata();

    // Get the column descriptor for the desired column (assuming column index 0)
    let column_desc = parquet_metadata
        .file_metadata()
        .schema_descr_ptr()
        .column(column_idx);

    // Get the page for the desired column chunk and page index
    let page: Page = match get_page_by_idx(file, row_group_idx, column_idx, page_idx) {
        Ok(Some(page)) => page,
        Ok(None) => {
            log::warn!(
                "No page found for row group {}, column {}, page {}",
                row_group_idx,
                column_idx,
                page_idx
            );
            return Ok(None);
        }
        Err(e) => {
            log::error!("Error retrieving page: {:?}", e);
            return Err(e);
        }
    };

    let bytes = match page.buffer().get_bytes(0, page.buffer().len()) {
        Ok(bytes) => bytes,
        Err(e) => {
            log::error!("Error getting bytes from page buffer: {:?}", e);
            return Err(e);
        }
    };

    // Create a ByteArrayColumnValueDecoder for the column
    let mut decoder: ByteArrayColumnValueDecoder<i32> =
        ByteArrayColumnValueDecoder::new(&column_desc);
    let max_def_level = column_desc.max_def_level();
    let num_levels = if max_def_level > 0 {
        // TODO: Read the definition levels from the page and count the number of non-zero levels
        // For now, assuming all values are present (no nulls)
        page.num_values() as usize
    } else {
        0
    };
    match decoder.set_data(
        page.encoding(),
        bytes.clone(),
        num_levels,
        Some(page.num_values() as usize),
    ) {
        Ok(_) => (),
        Err(e) => {
            log::error!("Error setting data in decoder: {:?}", e);
            return Err(e);
        }
    }
    // Read the values into an OffsetBuffer
    // let mut buffer = parquet::arrow::record_reader::buffer::OffsetBuffer::<i32>::default();
    let mut buffer = crate::arrow::buffer::offset_buffer::OffsetBuffer::default();

    println!(
        "Page metadata: num_values={}, encoding={:?}",
        page.num_values(),
        page.encoding()
    );
    println!(
        "Decoder metadata: num_levels={}, data_len={}",
        num_levels,
        bytes.len()
    );
    println!("Page buffer: {:?}", page.buffer());

    let _num_values = match decoder.read(&mut buffer, page.num_values() as usize) {
        Ok(num) => num,
        Err(e) => {
            log::error!("Error decoding byte array: {:?}", e);
            log::debug!(
                "Page metadata: num_values={}, encoding={:?}",
                page.num_values(),
                page.encoding()
            );
            log::debug!(
                "Decoder metadata: num_levels={}, data_len={}",
                num_levels,
                bytes.len()
            );
            return Err(e);
        }
    };

    // Convert the Vec<u8> to an Arrow Array
    let array = buffer.into_array(None, DataType::Binary);
    // let array = arrow::array::BinaryArray::from_vec(buffer);

    // Create a SchemaDescriptor from the Parquet schema
    let parquet_schema = parquet_metadata.file_metadata().schema_descr();

    // Create a RecordBatch from the array
    // Create a RecordBatch from the array
    let schema = parquet_to_arrow_schema(
        &parquet_schema,
        parquet_metadata.file_metadata().key_value_metadata(),
    )
    .unwrap();
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)]).unwrap();

    Ok(Some(batch))
}

/// an attempt to use record_read instead of ByteArray decoder to read records from pages
pub fn read_record_from_page(
    file: File,
    row_group_idx: usize,
    column_idx: usize,
    page_idx: usize,
) -> Result<Option<PrimitiveArray<arrow_array::types::Int32Type>>> {
    let file_reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
    let parquet_metadata = file_reader.metadata();

    // Get the column descriptor for the desired column
    let column_desc = parquet_metadata
        .file_metadata()
        .schema_descr_ptr()
        .column(column_idx);

    // Get the page for the desired column chunk and page index
    let page: Page = get_page_by_idx(file, row_group_idx, column_idx, page_idx)
        .unwrap()
        .unwrap();

    // println!("DEBUG INFO: num  of values in the page: {:?}", page.num_values());
    // Create a RecordReader for the column
    let mut record_reader: GenericRecordReader<Vec<i32>, ColumnValueDecoderImpl<Int32Type>> =
        RecordReader::<Int32Type>::new(column_desc.clone());

    // Create an InMemoryPageReader with the page data
    // println!("DEBUG INFO: page.buffer(): {:?}", page.buffer());
    let page_reader = Box::new(InMemoryPageReader::new(vec![page.clone()]));

    // Set the page reader for the record reader
    record_reader.set_page_reader(page_reader).unwrap();

    // Read all the records from the page
    let num_records_to_read = usize::try_from(page.num_values()).unwrap();
    let num_read = record_reader.read_records(num_records_to_read).unwrap();

    // if num_read != num_records_to_read {
    //     log::warn!(
    //         "Expected to read {} records, but only read {}",
    //         num_records_to_read,
    //         num_read
    //     );
    // } else {
    //     // println!("DEBUG INFO: num of records to read: {:?}", num_records_to_read);
    // }

    let record_data = record_reader.consume_record_data();

    // println!("DEBUG INFO: record_data: Vec<i32>: {:?}", record_data); // Vec data here, next is to transform into Arrow object
    // Create an Arrow array from the values
    let array = arrow_array::Int32Array::from(record_data);

    // step to create RecordBatch
    // Create a SchemaDescriptor from the Parquet schema
    // let parquet_schema = parquet_metadata.file_metadata().schema_descr();
    //
    // // Create a RecordBatch from the array
    // let schema = parquet_to_arrow_schema(
    //     &parquet_schema,
    //     parquet_metadata.file_metadata().key_value_metadata(),
    // ).unwrap();
    // let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)])
    //     .unwrap();
    // However, the column number in original parquet schema does not match the data we read
    // But Arrow array is enough.

    Ok(Some(array))
}

pub fn read_record_from_page_string(
    file: File,
    row_group_idx: usize,
    column_idx: usize,
    page_idx: usize,
) -> Result<Option<GenericByteArray<Utf8Type>>> {
    let file_reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
    let parquet_metadata = file_reader.metadata();

    // Get the column descriptor for the desired column
    let column_desc = parquet_metadata
        .file_metadata()
        .schema_descr_ptr()
        .column(column_idx);

    /// potentially we can infer the data type (INT32/BYTE_ARRAY) from column_desc
    /// let col_physical_type = column_desc.physical_type();
    // Get the page for the desired column chunk and page index
    let page: Page = get_page_by_idx(file, row_group_idx, column_idx, page_idx)
        .unwrap()
        .unwrap();

    // let mut record_reader = RecordReader::<ByteArrayType>::new(column_desc.clone());
    
    let page_reader = Box::new(InMemoryPageReader::new(vec![page.clone()]));
    
    // record_reader.set_page_reader(page_reader).expect("TODO: panic message");
    
    let column_reader: ColumnReader = get_column_reader(column_desc, page_reader);
    let mut typed_column_reader = get_typed_column_reader::<ByteArrayType>(column_reader);

    let num_records_to_read = usize::try_from(page.num_values()).unwrap();

    let mut values = Vec::new();
    let mut def_levels = Vec::new();
    let mut rep_levels = Vec::new();

    let (_, values_read, levels_read) = typed_column_reader
        .read_records(
            num_records_to_read,
            Some(&mut def_levels),
            Some(&mut rep_levels),
            &mut values,
        )
        .expect("read_batch() should be OK");


    let str_values: Vec<Option<&str>> = values.iter().map(|ba| Some(ba.as_utf8().unwrap())).collect();

    let array = GenericByteArray::<Utf8Type>::from(str_values);

    Ok(Some(array))
}

/// Reads a single page from a Parquet file and returns the data as an Arrow array
/// along with the number of rows in that page.
///
/// This function auto-detects the physical type of the column (INT32, INT64, or
/// BYTE_ARRAY/String) and uses the appropriate decoder to produce an Arrow array.
///
/// # Arguments
/// * `file` - An open file handle for the Parquet file.
/// * `row_group_idx` - The zero-based index of the row group to read from.
/// * `column_idx` - The zero-based index of the column to read.
/// * `page_idx` - The zero-based index of the page within the column chunk.
///
/// # Returns
/// A tuple of `(Arc<dyn Array>, usize)` where the array contains the decoded row-level
/// data and the `usize` is the number of rows in the page.
///
/// # Supported physical types
/// - `INT32` — returns an `Int32Array`
/// - `INT64` — returns an `Int64Array`
/// - `BYTE_ARRAY` — returns a `StringArray` (UTF-8)
///
/// # Errors
/// Returns `ParquetError` if the page cannot be read, the physical type is unsupported,
/// or decoding fails.
pub fn read_page_with_row_count(
    file: File,
    row_group_idx: usize,
    column_idx: usize,
    page_idx: usize,
) -> Result<(Arc<dyn Array>, usize)> {
    let file_reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
    let parquet_metadata = file_reader.metadata();

    let column_desc = parquet_metadata
        .file_metadata()
        .schema_descr_ptr()
        .column(column_idx);

    let physical_type = column_desc.physical_type();

    let page: Page = get_page_by_idx(file, row_group_idx, column_idx, page_idx)?
        .ok_or_else(|| {
            ParquetError::General(format!(
                "No page found for row_group={}, column={}, page={}",
                row_group_idx, column_idx, page_idx
            ))
        })?;

    match physical_type {
        PhysicalType::INT32 => {
            let mut record_reader: GenericRecordReader<Vec<i32>, ColumnValueDecoderImpl<Int32Type>> =
                RecordReader::<Int32Type>::new(column_desc.clone());
            let page_reader = Box::new(InMemoryPageReader::new(vec![page.clone()]));
            record_reader.set_page_reader(page_reader)?;
            let num_to_read = page.num_values() as usize;
            let num_read = record_reader.read_records(num_to_read)?;
            let data = record_reader.consume_record_data();
            let array = arrow_array::Int32Array::from(data);
            Ok((Arc::new(array) as Arc<dyn Array>, num_read))
        }
        PhysicalType::INT64 => {
            let mut record_reader: GenericRecordReader<Vec<i64>, ColumnValueDecoderImpl<Int64Type>> =
                RecordReader::<Int64Type>::new(column_desc.clone());
            let page_reader = Box::new(InMemoryPageReader::new(vec![page.clone()]));
            record_reader.set_page_reader(page_reader)?;
            let num_to_read = page.num_values() as usize;
            let num_read = record_reader.read_records(num_to_read)?;
            let data = record_reader.consume_record_data();
            let array = arrow_array::Int64Array::from(data);
            Ok((Arc::new(array) as Arc<dyn Array>, num_read))
        }
        PhysicalType::BYTE_ARRAY => {
            let page_reader = Box::new(InMemoryPageReader::new(vec![page.clone()]));
            let column_reader: ColumnReader = get_column_reader(column_desc, page_reader);
            let mut typed_reader = get_typed_column_reader::<ByteArrayType>(column_reader);
            let num_to_read = page.num_values() as usize;
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let mut rep_levels = Vec::new();
            let (_, values_read, _) = typed_reader
                .read_records(
                    num_to_read,
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut values,
                )?;
            let str_values: Vec<Option<&str>> = values
                .iter()
                .map(|ba| Some(ba.as_utf8().unwrap()))
                .collect();
            let array = GenericByteArray::<Utf8Type>::from(str_values);
            Ok((Arc::new(array) as Arc<dyn Array>, values_read))
        }
        _ => Err(ParquetError::General(format!(
            "Unsupported physical type {:?} for read_page_with_row_count",
            physical_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use arrow_cast::pretty::print_batches;

    use crate::basic::PageType;
    use crate::file::direct_page::{generate_random_page_indices_dataset_level, generate_random_page_indices_file_level, get_file_page_locations, get_page_by_idx, read_page_into_batch, read_page_with_row_count, read_record_from_page, read_record_from_page_string};
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::util::test_common::file_util::get_test_file;

    /// Helper to open a custom test fixture from test/fixtures/ directory.
    fn get_fixture_file(name: &str) -> File {
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("../../test/fixtures");
        path.push(name);
        File::open(&path).unwrap_or_else(|e| {
            panic!(
                "Fixture file {} not found at {}: {}. Run: uv run python test/fixtures/generate_test_parquet.py",
                name,
                path.display(),
                e
            )
        })
    }

    #[test]
    fn test_direct_access_page_by_idx() {
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");

        let page = get_page_by_idx(test_file, 0, 0, 0).unwrap().unwrap();

        assert_eq!(page.page_type(), PageType::DATA_PAGE);
    }

    #[test]
    fn test_correctness_of_locations_row_group_num() {
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");
        let file_reader = SerializedFileReader::new(test_file.try_clone().unwrap()).unwrap();
        let row_group_num_from_reader = file_reader.num_row_groups();
        println!("row group num from file reader: {row_group_num_from_reader}"); // 1
        let page_locations = get_file_page_locations(test_file.try_clone().unwrap()).unwrap();
        let row_group_num_from_locations = page_locations.unwrap().len();
        println!("row group num from location array: {row_group_num_from_locations}");

        assert_eq!(row_group_num_from_locations, row_group_num_from_reader);
    }

    #[test]
    fn test_file_page_num() {
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");

        let page_locations = get_file_page_locations(test_file.try_clone().unwrap())
            .unwrap()
            .unwrap();
        let page_num = page_locations[0].len();
        println!("the first row group contains {page_num} pages.");
        assert_ne!(page_num, 0);
    }

    #[test]
    fn test_correctness_of_locations_array_shuffle() {
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");
        let file_locations = get_file_page_locations(test_file.try_clone().unwrap())
            .unwrap()
            .unwrap();
        let column_idx = 0;
        let shuffled_indices =
            generate_random_page_indices_file_level(file_locations, column_idx).unwrap();

        let page = get_page_by_idx(
            test_file,
            shuffled_indices[0].0,
            column_idx,
            shuffled_indices[0].1,
        )
        .unwrap()
        .unwrap();

        assert_eq!(page.page_type(), PageType::DATA_PAGE);
    }

    #[test]
    fn test_correctness_dataset_locations_shuffle() {
        let test_file_0 = get_test_file("alltypes_tiny_pages_plain.parquet");
        let test_file_1 = get_test_file("alltypes_tiny_pages_plain.parquet");
        let test_file_2 = get_test_file("alltypes_tiny_pages_plain.parquet");

        let file_locations_0 = get_file_page_locations(test_file_0.try_clone().unwrap())
            .unwrap()
            .unwrap();
        let file_locations_1 = get_file_page_locations(test_file_1.try_clone().unwrap())
            .unwrap()
            .unwrap();
        let file_locations_2 = get_file_page_locations(test_file_2.try_clone().unwrap())
            .unwrap()
            .unwrap();

        let mut dataset_locations = Vec::new();
        dataset_locations.push(file_locations_0);
        dataset_locations.push(file_locations_1);
        dataset_locations.push(file_locations_2);

        let column_idx = 0;

        let random_indices =
            generate_random_page_indices_dataset_level(dataset_locations, column_idx).unwrap();

        let file_to_read = match random_indices[0].0 {
            0 => test_file_0,
            1 => test_file_1,
            2 => test_file_2,
            _ => panic!(),
        };

        let page = get_page_by_idx(
            file_to_read,
            random_indices[0].1,
            column_idx,
            random_indices[0].2,
        )
        .unwrap()
        .unwrap();
        let _buf = page.buffer(); // TODO: transform page buffer (data) to arrow/pyarrow object
        assert_eq!(page.page_type(), PageType::DATA_PAGE);
    }

    #[test]
    fn test_read_page_into_batch() {
        // alltypes_tiny_pages_plain.parquet has INT32 data in column 0 (id)
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");

        let row_group_idx = 0;
        let column_idx = 0;
        let page_idx = 0;

        let array = read_record_from_page(test_file, row_group_idx, column_idx, page_idx)
            .unwrap()
            .unwrap();
        assert!(array.len() > 0);
    }
    #[test]
    fn test_read_string_page_into_batch() {
        // alltypes_tiny_pages_plain.parquet has BYTE_ARRAY (string) data at column 9 (string_col)
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");

        let row_group_idx = 0;
        let column_idx = 9;
        let page_idx = 0;

        let array = read_record_from_page_string(test_file, row_group_idx, column_idx, page_idx)
            .unwrap()
            .unwrap();
    }

    #[test]
    fn test_read_page_with_row_count_int32() {
        // Use upstream test file: column 0 is INT32 (id)
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");
        let (array, row_count) = read_page_with_row_count(test_file, 0, 0, 0).unwrap();
        assert!(row_count > 0, "row_count should be positive");
        assert_eq!(array.len(), row_count, "array length should match row_count");
        assert_eq!(
            array.data_type(),
            &arrow_schema::DataType::Int32,
            "should produce Int32Array"
        );
    }

    #[test]
    fn test_read_page_with_row_count_string() {
        // Use upstream test file: column 9 is BYTE_ARRAY (string_col)
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");
        let (array, row_count) = read_page_with_row_count(test_file, 0, 9, 0).unwrap();
        assert!(row_count > 0, "row_count should be positive");
        assert_eq!(array.len(), row_count, "array length should match row_count");
        assert_eq!(
            array.data_type(),
            &arrow_schema::DataType::Utf8,
            "should produce StringArray"
        );
    }

    #[test]
    fn test_read_page_with_row_count_fixture_int32() {
        // Custom fixture: single_int32.parquet — 1 INT32 column, 2 row groups x 500 rows
        let file = get_fixture_file("single_int32.parquet");
        let page_locations = get_file_page_locations(file.try_clone().unwrap())
            .unwrap()
            .unwrap();

        // Sum row counts across all pages in row group 0 to verify they total 2000
        let num_pages_rg0 = page_locations[0][0].len();
        assert!(num_pages_rg0 > 1, "fixture should have multiple pages per column, got {}", num_pages_rg0);

        let mut total_rows_rg0 = 0;
        for page_idx in 0..num_pages_rg0 {
            let f = get_fixture_file("single_int32.parquet");
            let (array, row_count) = read_page_with_row_count(f, 0, 0, page_idx).unwrap();
            assert_eq!(array.len(), row_count);
            total_rows_rg0 += row_count;
        }
        assert_eq!(total_rows_rg0, 2000, "row group 0 should have exactly 2000 rows");
    }

    #[test]
    fn test_read_page_with_row_count_fixture_string() {
        // Custom fixture: single_string.parquet — 1 String column, 2 row groups x 500 rows
        let file = get_fixture_file("single_string.parquet");
        let page_locations = get_file_page_locations(file.try_clone().unwrap())
            .unwrap()
            .unwrap();

        let num_pages_rg0 = page_locations[0][0].len();
        assert!(num_pages_rg0 > 1, "fixture should have multiple pages per column");

        let mut total_rows_rg0 = 0;
        for page_idx in 0..num_pages_rg0 {
            let f = get_fixture_file("single_string.parquet");
            let (array, row_count) = read_page_with_row_count(f, 0, 0, page_idx).unwrap();
            assert_eq!(array.len(), row_count);
            total_rows_rg0 += row_count;
        }
        assert_eq!(total_rows_rg0, 2000, "row group 0 should have exactly 2000 rows");
    }

    #[test]
    fn test_read_page_with_row_count_fixture_multi_column() {
        // Custom fixture: multi_column.parquet — col 0: INT32, col 1: String, col 2: INT64
        // 2 row groups x 500 rows each
        let file = get_fixture_file("multi_column.parquet");
        let page_locations = get_file_page_locations(file.try_clone().unwrap())
            .unwrap()
            .unwrap();

        // Verify all 3 column types across both row groups
        for rg_idx in 0..2 {
            for (col_idx, expected_type) in [
                (0, arrow_schema::DataType::Int32),
                (1, arrow_schema::DataType::Utf8),
                (2, arrow_schema::DataType::Int64),
            ] {
                let num_pages = page_locations[rg_idx][col_idx].len();
                assert!(num_pages > 0, "should have at least one page");

                let mut total_rows = 0;
                for page_idx in 0..num_pages {
                    let f = get_fixture_file("multi_column.parquet");
                    let (array, row_count) =
                        read_page_with_row_count(f, rg_idx, col_idx, page_idx).unwrap();
                    assert_eq!(array.len(), row_count);
                    assert_eq!(
                        array.data_type(),
                        &expected_type,
                        "column {} should be {:?}",
                        col_idx,
                        expected_type
                    );
                    total_rows += row_count;
                }
                assert_eq!(
                    total_rows, 2000,
                    "row group {} column {} should have 2000 rows total",
                    rg_idx, col_idx
                );
            }
        }
    }
}
