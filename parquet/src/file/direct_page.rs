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

/// Reads aligned data from multiple columns for a given row group, returning arrays
/// that correspond to the same logical rows.
///
/// Given a row group and a page index (interpreted as the page index of the first
/// column in `column_indices`), this function determines the row range covered by that
/// page and then reads the corresponding rows from all requested columns.
///
/// Different columns may have different page boundaries. This function uses
/// `PageLocation.first_row_index` from the offset index to align reads across columns,
/// ensuring that the returned arrays all cover the same row range.
///
/// # Arguments
/// * `file` - An open file handle for the Parquet file.
/// * `row_group_idx` - The zero-based index of the row group to read from.
/// * `column_indices` - A list of zero-based column indices to read.
/// * `page_idx` - The page index within the first column of `column_indices`. This
///   determines the row range to read from all columns.
///
/// # Returns
/// A `Vec<(Arc<dyn Array>, usize)>` with one entry per requested column. Each entry
/// contains the decoded Arrow array and the number of rows, which will be identical
/// across all columns.
///
/// # Errors
/// Returns `ParquetError` if page locations are unavailable, a column index is out of
/// bounds, or any page read/decode fails.
pub fn read_multi_column_aligned(
    file: File,
    row_group_idx: usize,
    column_indices: &[usize],
    page_idx: usize,
) -> Result<Vec<(Arc<dyn Array>, usize)>> {
    if column_indices.is_empty() {
        return Ok(Vec::new());
    }

    let file_reader = SerializedFileReader::new(file.try_clone().unwrap())?;
    let parquet_metadata = file_reader.metadata();
    let row_group_meta = parquet_metadata.row_group(row_group_idx);
    let num_rows_in_rg = row_group_meta.num_rows() as usize;

    // Get page locations for all columns in this row group
    let row_group_reader = file_reader.get_row_group(row_group_idx)?;
    #[allow(deprecated)]
    let offset_indexes = read_offset_indexes(&file, row_group_reader.metadata().columns())?
        .ok_or_else(|| {
            ParquetError::General(
                "Offset index not available; file must be written with write_page_index=True"
                    .to_string(),
            )
        })?;

    let all_page_locations: Vec<Vec<PageLocation>> = offset_indexes
        .into_iter()
        .map(|idx| idx.page_locations().clone())
        .collect();

    // Determine the row range from the reference column (first in column_indices)
    let ref_col = column_indices[0];
    let ref_pages = &all_page_locations[ref_col];
    if page_idx >= ref_pages.len() {
        return Err(ParquetError::General(format!(
            "page_idx {} out of range for column {} which has {} pages",
            page_idx,
            ref_col,
            ref_pages.len()
        )));
    }

    let row_start = ref_pages[page_idx].first_row_index as usize;
    let row_end = if page_idx + 1 < ref_pages.len() {
        ref_pages[page_idx + 1].first_row_index as usize
    } else {
        num_rows_in_rg
    };
    let target_row_count = row_end - row_start;

    // For each requested column, find pages that overlap [row_start, row_end) and
    // read + slice them to produce exactly `target_row_count` rows.
    let mut results = Vec::with_capacity(column_indices.len());

    for &col_idx in column_indices {
        let col_pages = &all_page_locations[col_idx];
        let array = read_rows_from_column(
            &file,
            row_group_idx,
            col_idx,
            col_pages,
            row_start,
            row_end,
            num_rows_in_rg,
            parquet_metadata,
        )?;
        assert_eq!(
            array.len(),
            target_row_count,
            "column {} produced {} rows but expected {}",
            col_idx,
            array.len(),
            target_row_count
        );
        results.push((array, target_row_count));
    }

    Ok(results)
}

/// Generates shuffled page indices for multi-column aligned reading at the file level.
///
/// Unlike `generate_random_page_indices_file_level` which shuffles pages for a single
/// column, this function generates indices suitable for use with `read_multi_column_aligned`.
/// Pages are shuffled across all row groups in the file, where each index refers to a
/// page of the **reference column** (the first column in the column list passed to
/// `read_multi_column_aligned`).
///
/// # Arguments
/// * `file_page_locations` - Page locations for all row groups and columns, as returned
///   by `get_file_page_locations`. Shape: `[row_group][column][page]`.
/// * `reference_column_idx` - The column index used as the reference for page boundaries.
///   This should match the first element of `column_indices` passed to
///   `read_multi_column_aligned`.
///
/// # Returns
/// A shuffled `Vec<(usize, usize)>` of `(row_group_idx, page_idx)` pairs, where
/// `page_idx` refers to a page in the reference column.
pub fn generate_shuffled_multi_column_indices_file_level(
    file_page_locations: &Vec<Vec<Vec<PageLocation>>>,
    reference_column_idx: usize,
) -> Result<Vec<(usize, usize)>> {
    // This is functionally identical to generate_random_page_indices_file_level
    // because multi-column alignment is handled at read time by read_multi_column_aligned.
    // We shuffle pages of the reference column; other columns are aligned automatically.
    let num_row_groups = file_page_locations.len();
    let mut page_num_offsets = Vec::new();
    let mut total_page_num = 0;

    for row_group_idx in 0..num_row_groups {
        page_num_offsets.push(total_page_num);
        total_page_num += file_page_locations[row_group_idx][reference_column_idx].len();
    }

    let mut page_random_indices: Vec<usize> = (0..total_page_num).collect();
    page_random_indices.shuffle(&mut thread_rng());

    let mut random_rg_page_indices_pairs = Vec::new();
    for &page_random_index in &page_random_indices {
        let row_group_idx = page_num_offsets
            .binary_search(&page_random_index)
            .unwrap_or_else(|idx| idx - 1);
        let page_idx = page_random_index - page_num_offsets[row_group_idx];
        random_rg_page_indices_pairs.push((row_group_idx, page_idx));
    }
    Ok(random_rg_page_indices_pairs)
}

/// Generates shuffled page indices for multi-column aligned reading at the dataset level.
///
/// This extends `generate_shuffled_multi_column_indices_file_level` across multiple files,
/// producing shuffled `(file_idx, row_group_idx, page_idx)` triples suitable for use
/// with `read_multi_column_aligned`.
///
/// # Arguments
/// * `dataset_page_locations` - Page locations for all files, row groups, and columns.
///   Shape: `[file][row_group][column][page]`.
/// * `reference_column_idx` - The column index used as the reference for page boundaries.
///
/// # Returns
/// A shuffled `Vec<(usize, usize, usize)>` of `(file_idx, row_group_idx, page_idx)` triples.
pub fn generate_shuffled_multi_column_indices_dataset_level(
    dataset_page_locations: &Vec<Vec<Vec<Vec<PageLocation>>>>,
    reference_column_idx: usize,
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
            this_file_page_num +=
                dataset_page_locations[file_idx][row_group_idx][reference_column_idx].len();
        }

        page_num_offsets_within_file.push(this_file_page_num_offsets);
        page_num_offsets_across_file.push(total_page_num);
        total_page_num += this_file_page_num;
    }

    let mut page_random_indices: Vec<usize> = (0..total_page_num).collect();
    page_random_indices.shuffle(&mut thread_rng());

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

/// Reads rows in `[row_start, row_end)` from a single column by finding the overlapping
/// pages and slicing appropriately.
fn read_rows_from_column(
    file: &File,
    row_group_idx: usize,
    col_idx: usize,
    col_pages: &[PageLocation],
    row_start: usize,
    row_end: usize,
    num_rows_in_rg: usize,
    parquet_metadata: &crate::file::metadata::ParquetMetaData,
) -> Result<Arc<dyn Array>> {
    // Find all pages that overlap with [row_start, row_end)
    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

    for (p_idx, page_loc) in col_pages.iter().enumerate() {
        let p_row_start = page_loc.first_row_index as usize;
        let p_row_end = if p_idx + 1 < col_pages.len() {
            col_pages[p_idx + 1].first_row_index as usize
        } else {
            num_rows_in_rg
        };

        // Skip pages entirely before or after our target range
        if p_row_end <= row_start || p_row_start >= row_end {
            continue;
        }

        // Read the full page
        let f = file.try_clone().unwrap();
        let (page_array, _) = read_page_with_row_count(f, row_group_idx, col_idx, p_idx)?;

        // Compute the slice within this page that overlaps with [row_start, row_end)
        let slice_start = if row_start > p_row_start {
            row_start - p_row_start
        } else {
            0
        };
        let slice_end = if row_end < p_row_end {
            row_end - p_row_start
        } else {
            page_array.len()
        };

        let sliced = page_array.slice(slice_start, slice_end - slice_start);
        arrays.push(sliced);
    }

    // Concatenate all slices
    if arrays.is_empty() {
        return Err(ParquetError::General(format!(
            "No pages found for column {} covering rows [{}, {})",
            col_idx, row_start, row_end
        )));
    }

    if arrays.len() == 1 {
        Ok(arrays.into_iter().next().unwrap())
    } else {
        let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
        let concatenated = arrow_select::concat::concat(&refs).map_err(|e| {
            ParquetError::General(format!("Failed to concatenate arrays: {}", e))
        })?;
        Ok(concatenated)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use arrow_array::Array as _;
    use arrow_cast::pretty::print_batches;

    use crate::basic::PageType;
    use crate::file::direct_page::{generate_random_page_indices_dataset_level, generate_random_page_indices_file_level, generate_shuffled_multi_column_indices_file_level, generate_shuffled_multi_column_indices_dataset_level, get_file_page_locations, get_page_by_idx, get_page_by_location, read_multi_column_aligned, read_page_into_batch, read_page_with_row_count, read_record_from_page, read_record_from_page_string};
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

    #[test]
    fn test_read_multi_column_aligned_basic() {
        // multi_column.parquet: col 0 = INT32, col 1 = String, col 2 = INT64
        // Read all 3 columns aligned from row group 0, page 0
        let file = get_fixture_file("multi_column.parquet");
        let results = read_multi_column_aligned(file, 0, &[0, 1, 2], 0).unwrap();

        assert_eq!(results.len(), 3, "should return 3 columns");

        // All columns must have the same row count
        let row_count = results[0].1;
        assert!(row_count > 0, "row_count should be positive");
        for (i, (array, rc)) in results.iter().enumerate() {
            assert_eq!(*rc, row_count, "column {} row count mismatch", i);
            assert_eq!(array.len(), row_count, "column {} array length mismatch", i);
        }

        // Verify data types
        assert_eq!(results[0].0.data_type(), &arrow_schema::DataType::Int32);
        assert_eq!(results[1].0.data_type(), &arrow_schema::DataType::Utf8);
        assert_eq!(results[2].0.data_type(), &arrow_schema::DataType::Int64);
    }

    #[test]
    fn test_read_multi_column_aligned_all_pages_cover_full_row_group() {
        // Iterate through all pages (of reference column 0) and verify that
        // the total rows across all pages equals the row group row count (2000)
        let file = get_fixture_file("multi_column.parquet");
        let page_locations = get_file_page_locations(file.try_clone().unwrap())
            .unwrap()
            .unwrap();

        let num_pages_col0 = page_locations[0][0].len();
        assert!(num_pages_col0 > 1, "should have multiple pages");

        let mut total_rows = 0;
        for page_idx in 0..num_pages_col0 {
            let f = get_fixture_file("multi_column.parquet");
            let results = read_multi_column_aligned(f, 0, &[0, 1, 2], page_idx).unwrap();
            let row_count = results[0].1;
            // All columns should match
            for (_, rc) in &results {
                assert_eq!(*rc, row_count);
            }
            total_rows += row_count;
        }
        assert_eq!(total_rows, 2000, "all pages should cover all 2000 rows");
    }

    #[test]
    fn test_read_multi_column_aligned_two_columns() {
        // Read only 2 of 3 columns (INT32 and INT64, skipping String)
        let file = get_fixture_file("multi_column.parquet");
        let results = read_multi_column_aligned(file, 0, &[0, 2], 0).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, results[1].1, "row counts must match");
        assert_eq!(results[0].0.data_type(), &arrow_schema::DataType::Int32);
        assert_eq!(results[1].0.data_type(), &arrow_schema::DataType::Int64);
    }

    #[test]
    fn test_read_multi_column_aligned_row_group_1() {
        // Verify alignment works for row group 1 as well
        let file = get_fixture_file("multi_column.parquet");
        let results = read_multi_column_aligned(file, 1, &[0, 1, 2], 0).unwrap();

        assert_eq!(results.len(), 3);
        let row_count = results[0].1;
        assert!(row_count > 0);
        for (array, rc) in &results {
            assert_eq!(*rc, row_count);
            assert_eq!(array.len(), row_count);
        }
    }

    #[test]
    fn test_read_multi_column_aligned_data_correctness() {
        // Verify actual data values are correct and aligned
        // The fixture generates: col0 = row_idx (INT32), col2 = row_idx * 100 (INT64)
        let file = get_fixture_file("multi_column.parquet");
        let results = read_multi_column_aligned(file, 0, &[0, 2], 0).unwrap();

        let int32_array = results[0]
            .0
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        let int64_array = results[1]
            .0
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();

        // For each row, int64 value should be int32 value * 100
        for i in 0..int32_array.len() {
            let v32 = int32_array.value(i);
            let v64 = int64_array.value(i);
            assert_eq!(
                v64,
                v32 as i64 * 100,
                "row {}: INT64 ({}) should be INT32 ({}) * 100",
                i,
                v64,
                v32
            );
        }
    }

    #[test]
    fn test_shuffled_multi_column_file_level_covers_all_pages() {
        // Shuffled indices should cover every page of the reference column exactly once
        let file = get_fixture_file("multi_column.parquet");
        let page_locations = get_file_page_locations(file.try_clone().unwrap())
            .unwrap()
            .unwrap();

        let ref_col = 0;
        let shuffled = generate_shuffled_multi_column_indices_file_level(&page_locations, ref_col)
            .unwrap();

        // Total pages across all row groups for the reference column
        let total_pages: usize = page_locations.iter()
            .map(|rg| rg[ref_col].len())
            .sum();
        assert_eq!(shuffled.len(), total_pages, "should have one entry per page");

        // Every (rg, page) pair should appear exactly once
        let mut seen = std::collections::HashSet::new();
        for &(rg_idx, page_idx) in &shuffled {
            assert!(rg_idx < page_locations.len(), "rg_idx out of range");
            assert!(page_idx < page_locations[rg_idx][ref_col].len(), "page_idx out of range");
            assert!(seen.insert((rg_idx, page_idx)), "duplicate index ({}, {})", rg_idx, page_idx);
        }
        assert_eq!(seen.len(), total_pages);
    }

    #[test]
    fn test_shuffled_multi_column_file_level_aligned_reads() {
        // Use shuffled indices with read_multi_column_aligned and verify all rows are read
        // with correct alignment (INT64 = INT32 * 100)
        let file = get_fixture_file("multi_column.parquet");
        let page_locations = get_file_page_locations(file.try_clone().unwrap())
            .unwrap()
            .unwrap();

        let ref_col = 0;
        let shuffled = generate_shuffled_multi_column_indices_file_level(&page_locations, ref_col)
            .unwrap();

        let mut total_rows = 0;
        for &(rg_idx, page_idx) in &shuffled {
            let f = get_fixture_file("multi_column.parquet");
            let results = read_multi_column_aligned(f, rg_idx, &[0, 1, 2], page_idx).unwrap();

            let row_count = results[0].1;
            assert!(row_count > 0);
            // All columns must have the same row count
            for (_, rc) in &results {
                assert_eq!(*rc, row_count);
            }

            // Verify alignment: INT64 = INT32 * 100
            let int32_array = results[0].0.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
            let int64_array = results[2].0.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
            for i in 0..int32_array.len() {
                assert_eq!(int64_array.value(i), int32_array.value(i) as i64 * 100);
            }

            total_rows += row_count;
        }
        // 2 row groups x 2000 rows = 4000 total rows
        assert_eq!(total_rows, 4000, "shuffled reads should cover all rows");
    }

    #[test]
    fn test_shuffled_multi_column_dataset_level_covers_all_pages() {
        // Dataset-level shuffling across multiple "files" (same file used 3 times)
        let file0 = get_fixture_file("multi_column.parquet");
        let file1 = get_fixture_file("multi_column.parquet");
        let file2 = get_fixture_file("multi_column.parquet");

        let locs0 = get_file_page_locations(file0).unwrap().unwrap();
        let locs1 = get_file_page_locations(file1).unwrap().unwrap();
        let locs2 = get_file_page_locations(file2).unwrap().unwrap();

        let dataset_locations = vec![locs0, locs1, locs2];
        let ref_col = 0;
        let shuffled = generate_shuffled_multi_column_indices_dataset_level(&dataset_locations, ref_col)
            .unwrap();

        // Total pages across all files
        let total_pages: usize = dataset_locations.iter()
            .flat_map(|file_locs| file_locs.iter())
            .map(|rg| rg[ref_col].len())
            .sum();
        assert_eq!(shuffled.len(), total_pages);

        // Every (file, rg, page) triple should appear exactly once
        let mut seen = std::collections::HashSet::new();
        for &(file_idx, rg_idx, page_idx) in &shuffled {
            assert!(file_idx < dataset_locations.len());
            assert!(rg_idx < dataset_locations[file_idx].len());
            assert!(page_idx < dataset_locations[file_idx][rg_idx][ref_col].len());
            assert!(seen.insert((file_idx, rg_idx, page_idx)),
                "duplicate index ({}, {}, {})", file_idx, rg_idx, page_idx);
        }
        assert_eq!(seen.len(), total_pages);
    }

    #[test]
    fn test_shuffled_multi_column_dataset_level_aligned_reads() {
        // Dataset-level: verify shuffled reads produce correct aligned data
        let file = get_fixture_file("multi_column.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        let dataset_locations = vec![locs.clone(), locs.clone()];
        let ref_col = 0;
        let shuffled = generate_shuffled_multi_column_indices_dataset_level(&dataset_locations, ref_col)
            .unwrap();

        let mut total_rows = 0;
        for &(_, rg_idx, page_idx) in &shuffled {
            // All files are the same, so we can read from any
            let f = get_fixture_file("multi_column.parquet");
            let results = read_multi_column_aligned(f, rg_idx, &[0, 2], page_idx).unwrap();

            let row_count = results[0].1;
            assert_eq!(results[1].1, row_count);

            // Verify alignment
            let int32_array = results[0].0.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
            let int64_array = results[1].0.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
            for i in 0..int32_array.len() {
                assert_eq!(int64_array.value(i), int32_array.value(i) as i64 * 100);
            }

            total_rows += row_count;
        }
        // 2 files x 2 row groups x 2000 rows = 8000 total rows
        assert_eq!(total_rows, 8000, "dataset-level shuffled reads should cover all rows");
    }

    // ==================== Comprehensive tests for US-010 ====================

    // --- get_file_page_locations tests ---

    #[test]
    fn test_get_file_page_locations_fixture_structure() {
        // Verify the page locations structure: 2 row groups, 3 columns, multiple pages each
        let file = get_fixture_file("multi_column.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        assert_eq!(locs.len(), 2, "should have 2 row groups");
        for rg_idx in 0..2 {
            assert_eq!(locs[rg_idx].len(), 3, "each row group should have 3 columns");
            for col_idx in 0..3 {
                assert!(
                    locs[rg_idx][col_idx].len() > 1,
                    "rg {} col {} should have multiple pages, got {}",
                    rg_idx, col_idx, locs[rg_idx][col_idx].len()
                );
            }
        }
    }

    #[test]
    fn test_get_file_page_locations_single_column_file() {
        let file = get_fixture_file("single_int32.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        assert_eq!(locs.len(), 2, "should have 2 row groups");
        for rg_idx in 0..2 {
            assert_eq!(locs[rg_idx].len(), 1, "single-column file should have 1 column");
            assert!(locs[rg_idx][0].len() > 1, "should have multiple pages");
        }
    }

    #[test]
    fn test_get_file_page_locations_page_locations_monotonic() {
        // first_row_index should be strictly monotonically increasing within each column
        let file = get_fixture_file("multi_column.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        for rg_idx in 0..locs.len() {
            for col_idx in 0..locs[rg_idx].len() {
                let pages = &locs[rg_idx][col_idx];
                assert_eq!(pages[0].first_row_index, 0, "first page should start at row 0");
                for i in 1..pages.len() {
                    assert!(
                        pages[i].first_row_index > pages[i - 1].first_row_index,
                        "first_row_index should be strictly increasing: rg={} col={} page={} ({} <= {})",
                        rg_idx, col_idx, i, pages[i].first_row_index, pages[i - 1].first_row_index
                    );
                }
            }
        }
    }

    // --- get_page_by_idx tests ---

    #[test]
    fn test_get_page_by_idx_fixture_both_row_groups() {
        // Test page access from both row groups in fixture files
        let file = get_fixture_file("multi_column.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        for rg_idx in 0..2 {
            let num_pages = locs[rg_idx][0].len();
            for page_idx in 0..num_pages {
                let f = get_fixture_file("multi_column.parquet");
                let page = get_page_by_idx(f, rg_idx, 0, page_idx).unwrap().unwrap();
                assert_eq!(page.page_type(), PageType::DATA_PAGE);
                assert!(page.num_values() > 0, "page should have values");
            }
        }
    }

    #[test]
    fn test_get_page_by_idx_all_column_types() {
        // Access first page of each column type in multi_column.parquet
        for col_idx in 0..3 {
            let f = get_fixture_file("multi_column.parquet");
            let page = get_page_by_idx(f, 0, col_idx, 0).unwrap().unwrap();
            assert_eq!(page.page_type(), PageType::DATA_PAGE);
            assert!(page.num_values() > 0);
        }
    }

    // --- generate_random_page_indices_file_level tests ---

    #[test]
    fn test_random_indices_file_level_coverage() {
        // All page indices should be covered exactly once
        let file = get_fixture_file("multi_column.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        let column_idx = 0;
        let total_pages: usize = locs.iter().map(|rg| rg[column_idx].len()).sum();

        let indices = generate_random_page_indices_file_level(locs.clone(), column_idx).unwrap();
        assert_eq!(indices.len(), total_pages, "should cover all pages");

        // Every (rg, page) pair should appear exactly once
        let mut seen = std::collections::HashSet::new();
        for &(rg_idx, page_idx) in &indices {
            assert!(rg_idx < locs.len(), "rg_idx out of range");
            assert!(page_idx < locs[rg_idx][column_idx].len(), "page_idx out of range");
            assert!(seen.insert((rg_idx, page_idx)), "duplicate ({}, {})", rg_idx, page_idx);
        }
    }

    #[test]
    fn test_random_indices_file_level_different_columns() {
        // Test index generation for each column — different columns may have different page counts
        let file = get_fixture_file("multi_column.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        for col_idx in 0..3 {
            let total_pages: usize = locs.iter().map(|rg| rg[col_idx].len()).sum();
            let indices = generate_random_page_indices_file_level(locs.clone(), col_idx).unwrap();
            assert_eq!(indices.len(), total_pages, "column {} page count mismatch", col_idx);
        }
    }

    // --- generate_random_page_indices_dataset_level tests ---

    #[test]
    fn test_random_indices_dataset_level_coverage() {
        // All pages across all files should be covered exactly once
        let file0 = get_fixture_file("multi_column.parquet");
        let file1 = get_fixture_file("single_int32.parquet");

        let locs0 = get_file_page_locations(file0).unwrap().unwrap();
        let locs1 = get_file_page_locations(file1).unwrap().unwrap();

        let column_idx = 0;
        let dataset = vec![locs0.clone(), locs1.clone()];
        let total_pages: usize = dataset.iter()
            .flat_map(|f| f.iter())
            .map(|rg| rg[column_idx].len())
            .sum();

        let indices = generate_random_page_indices_dataset_level(dataset.clone(), column_idx).unwrap();
        assert_eq!(indices.len(), total_pages);

        let mut seen = std::collections::HashSet::new();
        for &(file_idx, rg_idx, page_idx) in &indices {
            assert!(file_idx < dataset.len());
            assert!(rg_idx < dataset[file_idx].len());
            assert!(page_idx < dataset[file_idx][rg_idx][column_idx].len());
            assert!(seen.insert((file_idx, rg_idx, page_idx)));
        }
    }

    // --- read_page_with_row_count additional tests ---

    #[test]
    fn test_read_page_with_row_count_fixture_int64() {
        // Custom fixture: multi_column.parquet, column 2 is INT64 (score)
        let file = get_fixture_file("multi_column.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        let col_idx = 2; // INT64 column
        let num_pages = locs[0][col_idx].len();
        let mut total_rows = 0;

        for page_idx in 0..num_pages {
            let f = get_fixture_file("multi_column.parquet");
            let (array, row_count) = read_page_with_row_count(f, 0, col_idx, page_idx).unwrap();
            assert_eq!(array.len(), row_count);
            assert_eq!(array.data_type(), &arrow_schema::DataType::Int64);
            total_rows += row_count;
        }
        assert_eq!(total_rows, 2000, "INT64 column should have 2000 rows in row group 0");
    }

    #[test]
    fn test_read_page_with_row_count_data_values_int32() {
        // Verify actual INT32 values are correct (fixture generates col0 = row_idx)
        let f = get_fixture_file("multi_column.parquet");
        let (array, row_count) = read_page_with_row_count(f, 0, 0, 0).unwrap();

        let int32_array = array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        // First page of row group 0 starts at row 0
        for i in 0..row_count {
            assert_eq!(int32_array.value(i), i as i32, "row {} should equal its index", i);
        }
    }

    #[test]
    fn test_read_page_with_row_count_data_values_int64() {
        // Verify actual INT64 values: col2 = row_idx * 100
        let f = get_fixture_file("multi_column.parquet");
        let (array, _) = read_page_with_row_count(f, 0, 2, 0).unwrap();

        let int64_array = array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        // The first page of INT64 column might not start at row 0 of the reference column,
        // but its own values should follow the pattern val = some_offset * 100
        // Since page 0 of each column starts at row 0 for the same row group:
        for i in 0..int64_array.len() {
            assert_eq!(int64_array.value(i), i as i64 * 100, "INT64 row {} should be {} * 100", i, i);
        }
    }

    #[test]
    fn test_read_page_with_row_count_data_values_string() {
        // Verify actual string values: col1 = "item_{row_idx:06d}"
        let f = get_fixture_file("multi_column.parquet");
        let (array, _) = read_page_with_row_count(f, 0, 1, 0).unwrap();

        let str_array = array.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        for i in 0..str_array.len() {
            let expected = format!("item_{:06}", i);
            assert_eq!(str_array.value(i), expected, "row {} string mismatch", i);
        }
    }

    #[test]
    fn test_read_page_with_row_count_row_group_1_offset() {
        // Row group 1 starts at row 2000. Verify col0 values start from 2000.
        let f = get_fixture_file("multi_column.parquet");
        let (array, _) = read_page_with_row_count(f, 1, 0, 0).unwrap();

        let int32_array = array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        // Row group 1 values should start at 2000 (the offset)
        assert_eq!(int32_array.value(0), 2000, "first value in row group 1 should be 2000");
    }

    // --- read_multi_column_aligned additional tests ---

    #[test]
    fn test_read_multi_column_aligned_empty_column_indices() {
        let file = get_fixture_file("multi_column.parquet");
        let results = read_multi_column_aligned(file, 0, &[], 0).unwrap();
        assert!(results.is_empty(), "empty column_indices should return empty results");
    }

    #[test]
    fn test_read_multi_column_aligned_single_column() {
        // Using multi-column aligned with a single column should still work
        let file = get_fixture_file("multi_column.parquet");
        let results = read_multi_column_aligned(file, 0, &[1], 0).unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].1 > 0);
        assert_eq!(results[0].0.data_type(), &arrow_schema::DataType::Utf8);
    }

    #[test]
    fn test_read_multi_column_aligned_string_data_correctness() {
        // Verify string column data is correctly aligned with INT32 column
        let file = get_fixture_file("multi_column.parquet");
        let results = read_multi_column_aligned(file, 0, &[0, 1], 0).unwrap();

        let int32_array = results[0].0.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        let str_array = results[1].0.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();

        for i in 0..int32_array.len() {
            let expected_name = format!("item_{:06}", int32_array.value(i));
            assert_eq!(
                str_array.value(i), expected_name,
                "row {}: string '{}' doesn't match INT32 value {}",
                i, str_array.value(i), int32_array.value(i)
            );
        }
    }

    #[test]
    fn test_read_multi_column_aligned_all_pages_row_group_1() {
        // Cover all pages in row group 1 and verify total rows
        let file = get_fixture_file("multi_column.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        let num_pages = locs[1][0].len();
        let mut total_rows = 0;

        for page_idx in 0..num_pages {
            let f = get_fixture_file("multi_column.parquet");
            let results = read_multi_column_aligned(f, 1, &[0, 1, 2], page_idx).unwrap();
            total_rows += results[0].1;
        }
        assert_eq!(total_rows, 2000, "row group 1 should have 2000 rows total");
    }

    #[test]
    fn test_read_multi_column_aligned_page_idx_out_of_range() {
        // Accessing a page index beyond the reference column's page count should error
        let file = get_fixture_file("multi_column.parquet");
        let result = read_multi_column_aligned(file, 0, &[0, 1, 2], 9999);
        assert!(result.is_err(), "out-of-range page_idx should return error");
    }

    // --- Shuffled multi-column index generation additional tests ---

    #[test]
    fn test_shuffled_multi_column_file_level_with_different_ref_columns() {
        // Test that shuffling works with different reference columns
        let file = get_fixture_file("multi_column.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        for ref_col in 0..3 {
            let total_pages: usize = locs.iter().map(|rg| rg[ref_col].len()).sum();
            let shuffled = generate_shuffled_multi_column_indices_file_level(&locs, ref_col).unwrap();
            assert_eq!(shuffled.len(), total_pages, "ref_col {} page count mismatch", ref_col);

            let mut seen = std::collections::HashSet::new();
            for &(rg, page) in &shuffled {
                assert!(seen.insert((rg, page)));
            }
            assert_eq!(seen.len(), total_pages);
        }
    }

    #[test]
    fn test_shuffled_multi_column_dataset_level_data_correctness() {
        // Verify that shuffled dataset-level indices produce correct data when read
        let file = get_fixture_file("multi_column.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        let dataset = vec![locs];
        let ref_col = 0;
        let shuffled = generate_shuffled_multi_column_indices_dataset_level(&dataset, ref_col).unwrap();

        // Collect all INT32 values across all shuffled pages
        let mut all_values = std::collections::HashSet::new();
        for &(_, rg_idx, page_idx) in &shuffled {
            let f = get_fixture_file("multi_column.parquet");
            let results = read_multi_column_aligned(f, rg_idx, &[0], page_idx).unwrap();
            let int32_array = results[0].0.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
            for i in 0..int32_array.len() {
                all_values.insert(int32_array.value(i));
            }
        }
        // With 1 file x 2 row groups x 2000 rows, we expect values 0..4000
        assert_eq!(all_values.len(), 4000, "should see all 4000 unique values");
    }

    // --- read_record_from_page additional tests ---

    #[test]
    fn test_read_record_from_page_fixture_int32() {
        // Test with fixture file for INT32 column
        let f = get_fixture_file("single_int32.parquet");
        let array = read_record_from_page(f, 0, 0, 0).unwrap().unwrap();
        assert!(!array.is_empty(), "should read some values");

        // Verify values start at 0 (fixture generates sequential integers)
        assert_eq!(array.value(0), 0, "first value should be 0");
    }

    #[test]
    fn test_read_record_from_page_string_fixture() {
        // Test with fixture file for string column
        let f = get_fixture_file("single_string.parquet");
        let array = read_record_from_page_string(f, 0, 0, 0).unwrap().unwrap();
        assert!(array.len() > 0, "should read some values");
    }

    // --- get_page_by_location tests ---

    #[test]
    fn test_get_page_by_location_direct() {
        // Test get_page_by_location by manually providing a page location
        let file = get_fixture_file("multi_column.parquet");
        let locs = get_file_page_locations(file.try_clone().unwrap()).unwrap().unwrap();

        let file_reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
        let row_group_reader = file_reader.get_row_group(0).unwrap();
        let column_meta = row_group_reader.metadata().column(0);

        let page_location = locs[0][0][0].clone();
        let page = get_page_by_location(file, page_location, column_meta).unwrap().unwrap();
        assert_eq!(page.page_type(), PageType::DATA_PAGE);
        assert!(page.num_values() > 0);
    }

    // --- Cross-function integration tests ---

    #[test]
    fn test_single_int32_file_end_to_end() {
        // End-to-end: get locations -> shuffle -> read all pages -> verify total rows
        let file = get_fixture_file("single_int32.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        let shuffled = generate_random_page_indices_file_level(locs, 0).unwrap();

        let mut total_rows = 0;
        for &(rg_idx, page_idx) in &shuffled {
            let f = get_fixture_file("single_int32.parquet");
            let (array, row_count) = read_page_with_row_count(f, rg_idx, 0, page_idx).unwrap();
            assert_eq!(array.len(), row_count);
            total_rows += row_count;
        }
        // 2 row groups x 2000 rows
        assert_eq!(total_rows, 4000, "should read all 4000 rows");
    }

    #[test]
    fn test_multi_column_end_to_end_all_rows_unique() {
        // End-to-end: verify all rows are read exactly once across shuffled pages
        let file = get_fixture_file("multi_column.parquet");
        let locs = get_file_page_locations(file).unwrap().unwrap();

        let shuffled = generate_shuffled_multi_column_indices_file_level(&locs, 0).unwrap();

        let mut all_ids = Vec::new();
        for &(rg_idx, page_idx) in &shuffled {
            let f = get_fixture_file("multi_column.parquet");
            let results = read_multi_column_aligned(f, rg_idx, &[0, 1, 2], page_idx).unwrap();

            let int32_array = results[0].0.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
            let str_array = results[1].0.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
            let int64_array = results[2].0.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();

            for i in 0..int32_array.len() {
                let id = int32_array.value(i);
                let name = str_array.value(i);
                let score = int64_array.value(i);

                // Verify alignment
                assert_eq!(name, format!("item_{:06}", id));
                assert_eq!(score, id as i64 * 100);

                all_ids.push(id);
            }
        }

        // Sort and verify all IDs 0..4000 are present exactly once
        all_ids.sort();
        assert_eq!(all_ids.len(), 4000);
        for (i, &id) in all_ids.iter().enumerate() {
            assert_eq!(id, i as i32, "missing or duplicate id at position {}", i);
        }
    }
}
