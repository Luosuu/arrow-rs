use arrow_array::types::Utf8Type;
use arrow_array::{Array, GenericByteArray, PrimitiveArray, RecordBatch};
use arrow_schema::DataType;
use rand::seq::SliceRandom;
use rand::rng;
use std::sync::Arc;
use std::fs::File;

use crate::arrow::array_reader::byte_array::ByteArrayColumnValueDecoder;
use crate::arrow::parquet_to_arrow_schema;
use crate::arrow::record_reader::{GenericRecordReader, RecordReader};
use crate::column::page::Page;
use crate::column::reader::{ColumnReader, get_column_reader, get_typed_column_reader};
use crate::column::reader::decoder::{ColumnValueDecoder, ColumnValueDecoderImpl};
use crate::compression::create_codec;
use crate::basic::{ConvertedType, Type as PhysicalType};
use crate::data_type::{ByteArrayType, FloatType, Int32Type, Int64Type};
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

/// Returns page locations for every column in every row group of a Parquet file.
///
/// The result is shaped as `[row_group][column][page]`, where each `PageLocation`
/// contains the byte offset, compressed page size, and first row index of the page.
///
/// # Arguments
/// * `file` - An open file handle for the Parquet file.
///
/// # Returns
/// `Some(locations)` if offset indexes are present, nested as row_group × column × page.
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

/// Generates randomly shuffled page indices for single-column reading at the file level.
///
/// Produces `(row_group_idx, page_idx)` pairs covering every page of the given column,
/// in a random order. Used for page-level shuffling in single-column datasets.
///
/// # Arguments
/// * `file_page_locations` - Page locations as returned by `get_file_page_locations`.
/// * `column_idx` - The column index to shuffle pages for.
pub fn generate_random_page_indices_file_level(
    file_page_locations: Vec<Vec<Vec<PageLocation>>>,
    column_idx: usize,
) -> Result<Vec<(usize, usize)>> {
    let mut page_num_offsets = Vec::new();
    let mut total_page_num = 0;

    for rg_pages in &file_page_locations {
        page_num_offsets.push(total_page_num);
        total_page_num += rg_pages[column_idx].len();
    }

    let mut page_random_indices: Vec<usize> = (0..total_page_num).collect();
    page_random_indices.shuffle(&mut rng());

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

/// Generates randomly shuffled page indices for single-column reading at the dataset level.
///
/// Extends `generate_random_page_indices_file_level` across multiple files, producing
/// `(file_idx, row_group_idx, page_idx)` triples in random order.
///
/// # Arguments
/// * `dataset_page_locations` - Page locations for all files. Shape: `[file][row_group][column][page]`.
/// * `column_idx` - The column index to shuffle pages for.
pub fn generate_random_page_indices_dataset_level(
    dataset_page_locations: Vec<Vec<Vec<Vec<PageLocation>>>>,
    column_idx: usize,
) -> Result<Vec<(usize, usize, usize)>> {
    let mut page_num_offsets_across_file = Vec::new();
    let mut page_num_offsets_within_file = Vec::new();
    let mut total_page_num = 0;
    for file_pages in &dataset_page_locations {
        let mut this_file_page_num_offsets = Vec::new();
        let mut this_file_page_num = 0;

        for rg_pages in file_pages {
            this_file_page_num_offsets.push(this_file_page_num);
            this_file_page_num += rg_pages[column_idx].len();
        }

        page_num_offsets_within_file.push(this_file_page_num_offsets);
        page_num_offsets_across_file.push(total_page_num);
        total_page_num += this_file_page_num;
    }

    // Generate random page indices
    let mut page_random_indices: Vec<usize> = (0..total_page_num).collect();
    page_random_indices.shuffle(&mut rng());

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

/// Reads and decodes a single Parquet page at the given byte location.
///
/// # Arguments
/// * `file` - An open file handle for the Parquet file.
/// * `page_location` - The byte offset, compressed size, and first row index of the page.
/// * `column_meta` - Column chunk metadata (needed for physical type and compression codec).
pub fn get_page_by_location(
    file: File,
    page_location: PageLocation,
    column_meta: &ColumnChunkMetaData,
) -> Result<Option<Page>> {
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

    let physical_type = column_meta.column_type();
    let props = Arc::new(ReaderProperties::builder().build());
    let decompressor = &mut create_codec(column_meta.compression(), props.codec_options())?;

    let page = decode_page(page_header, bytes, physical_type, decompressor.as_mut())?;

    Ok(Some(page))
}

/// Reads and decodes a single Parquet page by its row group, column, and page index.
///
/// Looks up the page location from the file's offset index, then delegates to
/// `get_page_by_location` for the actual read and decode.
///
/// # Arguments
/// * `file` - An open file handle for the Parquet file.
/// * `row_group_idx` - Zero-based row group index.
/// * `column_idx` - Zero-based column index.
/// * `page_idx` - Zero-based page index within the column chunk.
pub fn get_page_by_idx(
    file: File,
    row_group_idx: usize,
    column_idx: usize,
    page_idx: usize,
) -> Result<Option<Page>> {
    let file_clone = file.try_clone().unwrap();
    let file_reader = SerializedFileReader::new(file_clone).unwrap();
    let row_group_reader = file_reader.get_row_group(row_group_idx)?;

    #[allow(deprecated)]
    let offset_indexes = read_offset_indexes(&file, row_group_reader.metadata().columns())?;
    let page_locations = offset_indexes.as_ref().unwrap();
    let page_location = &page_locations[column_idx].page_locations()[page_idx];

    let column_meta = row_group_reader.metadata().column(column_idx);
    get_page_by_location(file, page_location.clone(), column_meta)
}

/// Reads a page into a RecordBatch using the ByteArray decoder.
///
/// **Known issue:** Creates a RecordBatch with the full file schema but only one
/// column of data, causing a schema mismatch. Prefer `read_page_with_row_count`
/// for production use.
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
    let mut buffer = crate::arrow::buffer::offset_buffer::OffsetBuffer::default();

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

    let array = buffer.into_array(None, DataType::Binary);

    let parquet_schema = parquet_metadata.file_metadata().schema_descr();
    let schema = parquet_to_arrow_schema(
        parquet_schema,
        parquet_metadata.file_metadata().key_value_metadata(),
    )
    .unwrap();
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)]).unwrap();

    Ok(Some(batch))
}

/// Reads an INT32 page and returns its data as an `Int32Array`.
///
/// Uses a `RecordReader` to decode the page. For a type-agnostic alternative,
/// see `read_page_with_row_count`.
///
/// # Arguments
/// * `file` - An open file handle for the Parquet file.
/// * `row_group_idx` - Zero-based row group index.
/// * `column_idx` - Zero-based column index (must be INT32).
/// * `page_idx` - Zero-based page index within the column chunk.
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

    let mut record_reader: GenericRecordReader<Vec<i32>, ColumnValueDecoderImpl<Int32Type>> =
        RecordReader::<Int32Type>::new(column_desc.clone());

    let page_reader = Box::new(InMemoryPageReader::new(vec![page.clone()]));
    record_reader.set_page_reader(page_reader).unwrap();

    let num_records_to_read = usize::try_from(page.num_values()).unwrap();
    let _num_read = record_reader.read_records(num_records_to_read).unwrap();

    let record_data = record_reader.consume_record_data();
    let array = arrow_array::Int32Array::from(record_data);

    Ok(Some(array))
}

/// Reads a BYTE_ARRAY (String) page and returns its data as a `StringArray`.
///
/// Uses a typed column reader to decode byte array values and interpret them
/// as UTF-8 strings. For a type-agnostic alternative, see `read_page_with_row_count`.
///
/// # Arguments
/// * `file` - An open file handle for the Parquet file.
/// * `row_group_idx` - Zero-based row group index.
/// * `column_idx` - Zero-based column index (must be BYTE_ARRAY).
/// * `page_idx` - Zero-based page index within the column chunk.
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

    let page: Page = get_page_by_idx(file, row_group_idx, column_idx, page_idx)
        .unwrap()
        .unwrap();

    let page_reader = Box::new(InMemoryPageReader::new(vec![page.clone()]));
    let column_reader: ColumnReader = get_column_reader(column_desc, page_reader);
    let mut typed_column_reader = get_typed_column_reader::<ByteArrayType>(column_reader);

    let num_records_to_read = usize::try_from(page.num_values()).unwrap();

    let mut values = Vec::new();
    let mut def_levels = Vec::new();
    let mut rep_levels = Vec::new();

    let (_, _values_read, _levels_read) = typed_column_reader
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
            let column_reader: ColumnReader = get_column_reader(column_desc.clone(), page_reader);
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

            // Distinguish string vs binary by checking the logical/converted type
            let is_string = column_desc.converted_type() == ConvertedType::UTF8
                || matches!(
                    column_desc.logical_type_ref(),
                    Some(crate::basic::LogicalType::String)
                );

            if is_string {
                let str_values: Vec<Option<&str>> = values
                    .iter()
                    .map(|ba| Some(ba.as_utf8().unwrap()))
                    .collect();
                let array = GenericByteArray::<Utf8Type>::from(str_values);
                Ok((Arc::new(array) as Arc<dyn Array>, values_read))
            } else {
                let bin_values: Vec<Option<&[u8]>> = values
                    .iter()
                    .map(|ba| Some(ba.data()))
                    .collect();
                let array = arrow_array::BinaryArray::from(bin_values);
                Ok((Arc::new(array) as Arc<dyn Array>, values_read))
            }
        }
        PhysicalType::FLOAT => {
            let mut record_reader: GenericRecordReader<Vec<f32>, ColumnValueDecoderImpl<FloatType>> =
                RecordReader::<FloatType>::new(column_desc.clone());
            let page_reader = Box::new(InMemoryPageReader::new(vec![page.clone()]));
            record_reader.set_page_reader(page_reader)?;
            let num_to_read = page.num_values() as usize;
            let num_read = record_reader.read_records(num_to_read)?;
            let data = record_reader.consume_record_data();
            let array = arrow_array::Float32Array::from(data);
            Ok((Arc::new(array) as Arc<dyn Array>, num_read))
        }
        _ => Err(ParquetError::General(format!(
            "Unsupported physical type {:?} for read_page_with_row_count",
            physical_type
        ))),
    }
}

/// Reads a page from a LIST<FLOAT32> column and returns the data as an Arrow
/// `ListArray<Float32>`, reconstructing list boundaries from the Parquet
/// repetition and definition levels.
///
/// The Parquet schema for `list<float32>` stores float values in a leaf column
/// with repetition level 1 (items within the same list) and definition levels
/// encoding nullability. This function reads the raw float values along with
/// their rep/def levels and rebuilds the list structure.
///
/// # Arguments
/// * `file` - An open file handle for the Parquet file.
/// * `row_group_idx` - The zero-based index of the row group to read from.
/// * `column_idx` - The zero-based index of the **leaf** column (the float
///   element inside the list).
/// * `page_idx` - The zero-based page index within the column chunk.
///
/// # Returns
/// A tuple of `(Arc<dyn Array>, usize)` where the array is a `ListArray<Float32>`
/// and `usize` is the number of **rows** (lists), not the number of float elements.
///
/// # Errors
/// Returns `ParquetError` if the column is not a FLOAT type or decoding fails.
pub fn read_list_float_page(
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

    let page: Page = get_page_by_idx(file, row_group_idx, column_idx, page_idx)?
        .ok_or_else(|| {
            ParquetError::General(format!(
                "No page found for row_group={}, column={}, page={}",
                row_group_idx, column_idx, page_idx
            ))
        })?;

    let mut record_reader: GenericRecordReader<Vec<f32>, ColumnValueDecoderImpl<FloatType>> =
        RecordReader::<FloatType>::new(column_desc.clone());
    let page_reader = Box::new(InMemoryPageReader::new(vec![page.clone()]));
    record_reader.set_page_reader(page_reader)?;
    let num_to_read = page.num_values() as usize;
    record_reader.read_records(num_to_read)?;

    let values = record_reader.consume_record_data();
    let rep_levels = record_reader.consume_rep_levels();
    let def_levels = record_reader.consume_def_levels();

    // Reconstruct ListArray from values + rep/def levels
    // rep_level=0 means start of a new top-level row (new list)
    // rep_level=1 means continuation within the same list
    let rep = rep_levels.unwrap_or_default();

    let max_def_level = column_desc.max_def_level();

    // Build offsets: each rep_level=0 starts a new list
    let mut offsets: Vec<i32> = Vec::new();
    let mut cur_offset: i32 = 0;
    let mut num_rows = 0;

    if !rep.is_empty() {
        for (i, &r) in rep.iter().enumerate() {
            if r == 0 {
                offsets.push(cur_offset);
                num_rows += 1;
            }
            // Only count values where def_level equals max_def_level (value is present)
            if let Some(ref dl) = def_levels {
                if dl[i] == max_def_level {
                    cur_offset += 1;
                }
            } else {
                cur_offset += 1;
            }
        }
    }
    offsets.push(cur_offset);

    // Build the Float32Array from the actual values (already null-padded by RecordReader)
    let float_array = arrow_array::Float32Array::from(values);

    // Build the ListArray
    let offsets_array = arrow_array::Int32Array::from(offsets);
    let offsets_buffer = arrow_buffer::OffsetBuffer::new(offsets_array.into_parts().1);
    let list_field = Arc::new(arrow_schema::Field::new("element", arrow_schema::DataType::Float32, true));
    let list_array = arrow_array::ListArray::new(list_field, offsets_buffer, Arc::new(float_array), None);

    Ok((Arc::new(list_array) as Arc<dyn Array>, num_rows))
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
        // For nested (list) columns, array.len() is the number of elements
        // which can exceed target_row_count. Only assert for flat columns.
        let col_desc = parquet_metadata
            .file_metadata()
            .schema_descr_ptr()
            .column(col_idx);
        if col_desc.max_rep_level() == 0 {
            assert_eq!(
                array.len(),
                target_row_count,
                "column {} produced {} rows but expected {}",
                col_idx,
                array.len(),
                target_row_count
            );
        }
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
    file_page_locations: &[Vec<Vec<PageLocation>>],
    reference_column_idx: usize,
) -> Result<Vec<(usize, usize)>> {
    // This is functionally identical to generate_random_page_indices_file_level
    // because multi-column alignment is handled at read time by read_multi_column_aligned.
    // We shuffle pages of the reference column; other columns are aligned automatically.
    let mut page_num_offsets = Vec::new();
    let mut total_page_num = 0;

    for rg_pages in file_page_locations {
        page_num_offsets.push(total_page_num);
        total_page_num += rg_pages[reference_column_idx].len();
    }

    let mut page_random_indices: Vec<usize> = (0..total_page_num).collect();
    page_random_indices.shuffle(&mut rng());

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
    dataset_page_locations: &[Vec<Vec<Vec<PageLocation>>>],
    reference_column_idx: usize,
) -> Result<Vec<(usize, usize, usize)>> {
    let mut page_num_offsets_across_file = Vec::new();
    let mut page_num_offsets_within_file = Vec::new();
    let mut total_page_num = 0;

    for file_pages in dataset_page_locations {
        let mut this_file_page_num_offsets = Vec::new();
        let mut this_file_page_num = 0;

        for rg_pages in file_pages {
            this_file_page_num_offsets.push(this_file_page_num);
            this_file_page_num +=
                rg_pages[reference_column_idx].len();
        }

        page_num_offsets_within_file.push(this_file_page_num_offsets);
        page_num_offsets_across_file.push(total_page_num);
        total_page_num += this_file_page_num;
    }

    let mut page_random_indices: Vec<usize> = (0..total_page_num).collect();
    page_random_indices.shuffle(&mut rng());

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

/// Reads an arbitrary range of rows `[start_row, end_row)` from a single column
/// in a given row group.
///
/// Uses page metadata (`PageLocation.first_row_index`) to find the overlapping pages,
/// reads them via `read_page_with_row_count`, slices to the exact row range, and
/// concatenates sliced arrays.
///
/// Supports INT32, INT64, FLOAT, BYTE_ARRAY (string and binary) physical types.
/// For LIST<FLOAT32> columns, this function reads the **element** column and returns
/// the flat float values in the range; use `read_list_float_page` for list-aware reads.
///
/// # Arguments
/// * `file` - An open file handle for the Parquet file.
/// * `row_group_idx` - Zero-based index of the row group to read from.
/// * `column_idx` - Zero-based index of the column to read.
/// * `start_row` - The inclusive start row within the row group.
/// * `end_row` - The exclusive end row within the row group.
///
/// # Returns
/// A tuple of `(Arc<dyn Array>, usize)` containing the sliced array and the number
/// of rows read (`end_row - start_row`).
///
/// # Errors
/// Returns `ParquetError` if the file lacks offset indexes, the row range is out of
/// bounds, or decoding fails.
pub fn read_row_range(
    file: File,
    row_group_idx: usize,
    column_idx: usize,
    start_row: usize,
    end_row: usize,
) -> Result<(Arc<dyn Array>, usize)> {
    if start_row >= end_row {
        return Err(ParquetError::General(format!(
            "Invalid row range: start_row ({}) must be less than end_row ({})",
            start_row, end_row
        )));
    }

    let file_reader = SerializedFileReader::new(file.try_clone().unwrap())?;
    let parquet_metadata = file_reader.metadata();
    let row_group_meta = parquet_metadata.row_group(row_group_idx);
    let num_rows_in_rg = row_group_meta.num_rows() as usize;

    if end_row > num_rows_in_rg {
        return Err(ParquetError::General(format!(
            "end_row ({}) exceeds row group size ({})",
            end_row, num_rows_in_rg
        )));
    }

    let row_group_reader = file_reader.get_row_group(row_group_idx)?;
    #[allow(deprecated)]
    let offset_indexes = read_offset_indexes(&file, row_group_reader.metadata().columns())?
        .ok_or_else(|| {
            ParquetError::General(
                "Offset index not available; file must be written with write_page_index=True"
                    .to_string(),
            )
        })?;

    let col_pages: Vec<PageLocation> = offset_indexes[column_idx].page_locations().clone();

    let array = read_rows_from_column(
        &file,
        row_group_idx,
        column_idx,
        &col_pages,
        start_row,
        end_row,
        num_rows_in_rg,
        parquet_metadata,
    )?;

    let row_count = end_row - start_row;
    Ok((array, row_count))
}

/// Reads a single row from a column in a row group, returning a length-1 array.
///
/// This is a convenience wrapper around [`read_row_range`] that reads exactly one row
/// at the given `row_idx`. Supports all physical types including LIST<FLOAT32>.
///
/// # Arguments
/// * `file` - An open file handle to the Parquet file
/// * `row_group_idx` - The row group index to read from
/// * `column_idx` - The column index to read
/// * `row_idx` - The zero-based row index within the row group
///
/// # Returns
/// A length-1 `Arc<dyn Array>` containing the value at the specified row.
pub fn read_single_row(
    file: File,
    row_group_idx: usize,
    column_idx: usize,
    row_idx: usize,
) -> Result<Arc<dyn Array>> {
    let (array, _row_count) = read_row_range(file, row_group_idx, column_idx, row_idx, row_idx + 1)?;
    Ok(array)
}

/// Reads rows in `[row_start, row_end)` from a single column by finding the overlapping
/// pages and slicing appropriately.
#[allow(clippy::too_many_arguments)]
fn read_rows_from_column(
    file: &File,
    row_group_idx: usize,
    col_idx: usize,
    col_pages: &[PageLocation],
    row_start: usize,
    row_end: usize,
    num_rows_in_rg: usize,
    _parquet_metadata: &crate::file::metadata::ParquetMetaData,
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
        let slice_start = row_start.saturating_sub(p_row_start);
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
    use std::sync::Arc;

    use arrow_array::Array as _;
    use arrow_cast::pretty::print_batches;

    use crate::basic::PageType;
    use crate::file::direct_page::{generate_random_page_indices_dataset_level, generate_random_page_indices_file_level, generate_shuffled_multi_column_indices_file_level, generate_shuffled_multi_column_indices_dataset_level, get_file_page_locations, get_page_by_idx, get_page_by_location, read_multi_column_aligned, read_page_into_batch, read_page_with_row_count, read_record_from_page, read_record_from_page_string, read_row_range, read_single_row};
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

    // ==================== Binary (BYTE_ARRAY) column tests for US-003 ====================

    /// Helper to open the Libero fixture file.
    fn get_libero_fixture() -> File {
        get_fixture_file("libero_fixture.parquet")
    }

    #[test]
    fn test_read_binary_column_from_libero_fixture() {
        // Libero fixture col 0 = observation.images.image.bytes (BYTE_ARRAY, binary)
        let file = get_libero_fixture();
        let (array, row_count) = read_page_with_row_count(file, 0, 0, 0).unwrap();

        assert!(row_count > 0, "should read some rows");
        assert_eq!(array.len(), row_count);
        assert_eq!(
            array.data_type(),
            &arrow_schema::DataType::Binary,
            "binary BYTE_ARRAY should produce BinaryArray, not StringArray"
        );

        // Verify data is valid PNG bytes (starts with PNG signature)
        let bin_array = array.as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap();
        let first_value = bin_array.value(0);
        assert!(first_value.len() > 8, "PNG bytes should be non-trivial");
        assert_eq!(&first_value[..4], b"\x89PNG", "should be valid PNG data");
    }

    #[test]
    fn test_read_binary_vs_string_column_type_distinction() {
        // Col 0 (bytes) should be Binary, col 1 (path) should be Utf8/String
        let file = get_libero_fixture();
        let locs = get_file_page_locations(file).unwrap().unwrap();

        // Col 0: binary
        let f = get_libero_fixture();
        let (binary_array, _) = read_page_with_row_count(f, 0, 0, 0).unwrap();
        assert_eq!(binary_array.data_type(), &arrow_schema::DataType::Binary,
            "col 0 (bytes) should be Binary");

        // Col 1: string
        let f = get_libero_fixture();
        let (string_array, _) = read_page_with_row_count(f, 0, 1, 0).unwrap();
        assert_eq!(string_array.data_type(), &arrow_schema::DataType::Utf8,
            "col 1 (path) should be Utf8/String");
    }

    #[test]
    fn test_read_binary_column_all_pages_row_count() {
        // Sum row counts across all pages of the binary column to verify total rows
        let file = get_libero_fixture();
        let locs = get_file_page_locations(file).unwrap().unwrap();

        for rg_idx in 0..locs.len() {
            let num_pages = locs[rg_idx][0].len();
            assert!(num_pages >= 1, "should have at least 1 page");

            let mut total_rows = 0;
            for page_idx in 0..num_pages {
                let f = get_libero_fixture();
                let (array, row_count) = read_page_with_row_count(f, rg_idx, 0, page_idx).unwrap();
                assert_eq!(array.len(), row_count);
                assert_eq!(array.data_type(), &arrow_schema::DataType::Binary);
                total_rows += row_count;
            }
            assert_eq!(total_rows, 50,
                "row group {} should have 50 rows (frames per episode)", rg_idx);
        }
    }

    #[test]
    fn test_read_multi_column_aligned_with_binary() {
        // Read binary column (col 0) alongside INT64 columns (cols 4, 5)
        let file = get_libero_fixture();
        let results = read_multi_column_aligned(file, 0, &[0, 4, 5], 0).unwrap();

        assert_eq!(results.len(), 3);

        // All columns must have the same row count
        let row_count = results[0].1;
        assert!(row_count > 0);
        for (i, (array, rc)) in results.iter().enumerate() {
            assert_eq!(*rc, row_count, "column {} row count mismatch", i);
            assert_eq!(array.len(), row_count);
        }

        // Verify types
        assert_eq!(results[0].0.data_type(), &arrow_schema::DataType::Binary,
            "col 0 should be Binary");
        assert_eq!(results[1].0.data_type(), &arrow_schema::DataType::Int64,
            "col 4 (frame_index) should be Int64");
        assert_eq!(results[2].0.data_type(), &arrow_schema::DataType::Int64,
            "col 5 (episode_index) should be Int64");

        // Verify binary data contains valid PNGs
        let bin_array = results[0].0.as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap();
        for i in 0..bin_array.len() {
            let value = bin_array.value(i);
            assert_eq!(&value[..4], b"\x89PNG", "row {} should be valid PNG", i);
        }
    }

    #[test]
    fn test_read_multi_column_aligned_binary_and_string() {
        // Read binary col (0) and string col (1) together — both BYTE_ARRAY but different logical types
        let file = get_libero_fixture();
        let results = read_multi_column_aligned(file, 0, &[0, 1], 0).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, results[1].1, "row counts must match");

        assert_eq!(results[0].0.data_type(), &arrow_schema::DataType::Binary);
        assert_eq!(results[1].0.data_type(), &arrow_schema::DataType::Utf8);

        // Verify string values are file paths
        let str_array = results[1].0.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert!(str_array.value(0).starts_with("frame_"),
            "path should start with 'frame_', got '{}'", str_array.value(0));
    }

    // ==================== LIST<FLOAT32> column tests for US-004 ====================

    #[test]
    fn test_read_float_column_from_libero_fixture() {
        // Libero fixture col 2 = observation.state element (FLOAT physical type)
        // For list columns, array.len() > row_count because array holds all elements
        // while row_count reflects the number of top-level records.
        let file = get_libero_fixture();
        let (array, row_count) = read_page_with_row_count(file, 0, 2, 0).unwrap();

        assert!(row_count > 0, "should read some records");
        // For list<float32> with 8 elements per row, array has 8x more elements than rows
        assert!(array.len() >= row_count,
            "array.len()={} should be >= row_count={}", array.len(), row_count);
        assert_eq!(
            array.data_type(),
            &arrow_schema::DataType::Float32,
            "FLOAT physical type should produce Float32Array"
        );
    }

    #[test]
    fn test_read_list_float_page_state_column() {
        // Libero fixture col 2 = observation.state list element (FLOAT)
        // State has 8 floats per frame, 50 frames in row group 0
        let file = get_libero_fixture();
        let (array, num_rows) = super::read_list_float_page(file, 0, 2, 0).unwrap();

        assert!(num_rows > 0, "should have at least one row (list)");
        assert_eq!(
            array.data_type(),
            &arrow_schema::DataType::List(
                Arc::new(arrow_schema::Field::new("element", arrow_schema::DataType::Float32, true))
            ),
            "should produce ListArray<Float32>"
        );

        let list_array = array.as_any().downcast_ref::<arrow_array::ListArray>().unwrap();
        assert_eq!(list_array.len(), num_rows);

        // Each list should have 8 elements (state dimension)
        for i in 0..list_array.len() {
            let inner = list_array.value(i);
            assert_eq!(inner.len(), 8,
                "row {} state should have 8 elements, got {}", i, inner.len());
        }
    }

    #[test]
    fn test_read_list_float_page_action_column() {
        // Libero fixture col 3 = action list element (FLOAT)
        // Action has 7 floats per frame
        let file = get_libero_fixture();
        let (array, num_rows) = super::read_list_float_page(file, 0, 3, 0).unwrap();

        assert!(num_rows > 0);
        let list_array = array.as_any().downcast_ref::<arrow_array::ListArray>().unwrap();

        // Each list should have 7 elements (action dimension)
        for i in 0..list_array.len() {
            let inner = list_array.value(i);
            assert_eq!(inner.len(), 7,
                "row {} action should have 7 elements, got {}", i, inner.len());
        }
    }

    #[test]
    fn test_read_list_float_page_state_values_correctness() {
        // Verify actual float values match the fixture generation formula:
        // state[i] = episode_idx * 0.1 + frame_idx * 0.01 + element_idx * 0.001
        // Row group 0 = episode 0
        let file = get_libero_fixture();
        let (array, num_rows) = super::read_list_float_page(file, 0, 2, 0).unwrap();

        let list_array = array.as_any().downcast_ref::<arrow_array::ListArray>().unwrap();

        let episode_idx = 0;
        for frame_idx in 0..num_rows {
            let inner = list_array.value(frame_idx);
            let float_array = inner.as_any().downcast_ref::<arrow_array::Float32Array>().unwrap();
            for elem_idx in 0..8 {
                let expected = episode_idx as f32 * 0.1 + frame_idx as f32 * 0.01 + elem_idx as f32 * 0.001;
                let actual = float_array.value(elem_idx);
                assert!(
                    (actual - expected).abs() < 1e-5,
                    "state[{}][{}] = {} but expected {}",
                    frame_idx, elem_idx, actual, expected
                );
            }
        }
    }

    #[test]
    fn test_read_list_float_page_all_pages_row_count() {
        // Sum row counts across all pages of state column to verify total
        let file = get_libero_fixture();
        let locs = get_file_page_locations(file).unwrap().unwrap();

        for rg_idx in 0..locs.len() {
            let num_pages = locs[rg_idx][2].len();
            let mut total_rows = 0;
            for page_idx in 0..num_pages {
                let f = get_libero_fixture();
                let (array, num_rows) = super::read_list_float_page(f, rg_idx, 2, page_idx).unwrap();
                let list_array = array.as_any().downcast_ref::<arrow_array::ListArray>().unwrap();
                assert_eq!(list_array.len(), num_rows);
                total_rows += num_rows;
            }
            assert_eq!(total_rows, 50,
                "row group {} should have 50 rows (frames)", rg_idx);
        }
    }

    #[test]
    fn test_read_multi_column_aligned_float_columns_only() {
        // Read two FLOAT list element columns together (state col 2, action col 3).
        // Both are list element columns with the same number of rows, so they
        // should align correctly (even though element counts differ: 8 vs 7 per row).
        let file = get_libero_fixture();
        let results = read_multi_column_aligned(file, 0, &[2, 3], 0).unwrap();

        assert_eq!(results.len(), 2);

        // Both should have the same row_count (top-level records)
        let row_count = results[0].1;
        assert!(row_count > 0);
        assert_eq!(results[1].1, row_count, "both columns should report same row count");

        assert_eq!(results[0].0.data_type(), &arrow_schema::DataType::Float32,
            "col 2 (state element) should be Float32");
        assert_eq!(results[1].0.data_type(), &arrow_schema::DataType::Float32,
            "col 3 (action element) should be Float32");

        // State has 8 elements per row, action has 7
        // Total elements: row_count * 8 for state, row_count * 7 for action
        assert_eq!(results[0].0.len(), row_count * 8,
            "state should have 8 elements per row");
        assert_eq!(results[1].0.len(), row_count * 7,
            "action should have 7 elements per row");
    }

    #[test]
    fn test_read_multi_column_aligned_non_list_columns() {
        // Read only non-list columns from Libero fixture (INT64 columns)
        // col 4 = frame_index, col 5 = episode_index
        let file = get_libero_fixture();
        let results = read_multi_column_aligned(file, 0, &[4, 5], 0).unwrap();

        assert_eq!(results.len(), 2);
        let row_count = results[0].1;
        assert!(row_count > 0);
        assert_eq!(results[1].1, row_count);
        assert_eq!(results[0].0.len(), row_count);
        assert_eq!(results[1].0.len(), row_count);
        assert_eq!(results[0].0.data_type(), &arrow_schema::DataType::Int64);
        assert_eq!(results[1].0.data_type(), &arrow_schema::DataType::Int64);
    }

    #[test]
    fn test_read_list_float_page_row_group_1() {
        // Verify list reading works for row group 1 (episode 1)
        let file = get_libero_fixture();
        let (array, num_rows) = super::read_list_float_page(file, 1, 2, 0).unwrap();

        let list_array = array.as_any().downcast_ref::<arrow_array::ListArray>().unwrap();
        assert!(num_rows > 0);

        // First frame in row group 1: episode_idx=1, frame_idx=0
        // state[0] = 1.0 * 0.1 + 0 * 0.01 + 0 * 0.001 = 0.1
        let first_list = list_array.value(0);
        let float_array = first_list.as_any().downcast_ref::<arrow_array::Float32Array>().unwrap();
        let expected_first = 1.0_f32 * 0.1;
        assert!(
            (float_array.value(0) - expected_first).abs() < 1e-5,
            "first state value in rg1 should be ~{}, got {}",
            expected_first, float_array.value(0)
        );
    }

    // ========================================================================
    // Tests for read_row_range
    // ========================================================================

    #[test]
    fn test_read_row_range_first_10_rows_int32() {
        // Read rows 0-10 from INT32 column in multi_column.parquet
        let file = get_fixture_file("multi_column.parquet");
        let (array, row_count) = read_row_range(file, 0, 0, 0, 10).unwrap();

        assert_eq!(row_count, 10);
        assert_eq!(array.len(), 10);

        let int_array = array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        // Data pattern: col0 (id) = row_idx
        for i in 0..10 {
            assert_eq!(int_array.value(i), i as i32);
        }
    }

    #[test]
    fn test_read_row_range_spanning_page_boundaries() {
        // multi_column.parquet has 2000 rows per RG, page_size=256, PLAIN encoding
        // INT32 pages hold ~64 values each (256 bytes / 4 bytes per i32)
        // Reading rows 50-150 should span multiple pages
        let file = get_fixture_file("multi_column.parquet");
        let (array, row_count) = read_row_range(file, 0, 0, 50, 150).unwrap();

        assert_eq!(row_count, 100);
        assert_eq!(array.len(), 100);

        let int_array = array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        for i in 0..100 {
            assert_eq!(int_array.value(i), (50 + i) as i32);
        }
    }

    #[test]
    fn test_read_row_range_last_n_rows() {
        // Read the last 20 rows of row group 0 (rows 1980-2000)
        let file = get_fixture_file("multi_column.parquet");
        let (array, row_count) = read_row_range(file, 0, 0, 1980, 2000).unwrap();

        assert_eq!(row_count, 20);
        assert_eq!(array.len(), 20);

        let int_array = array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        for i in 0..20 {
            assert_eq!(int_array.value(i), (1980 + i) as i32);
        }
    }

    #[test]
    fn test_read_row_range_int64() {
        // Read INT64 column (col 2, score = row_idx * 100)
        let file = get_fixture_file("multi_column.parquet");
        let (array, row_count) = read_row_range(file, 0, 2, 10, 20).unwrap();

        assert_eq!(row_count, 10);
        let int64_array = array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        for i in 0..10 {
            assert_eq!(int64_array.value(i), (10 + i) as i64 * 100);
        }
    }

    #[test]
    fn test_read_row_range_string() {
        // Read String column (col 1, name = "item_{row_idx:06d}")
        let file = get_fixture_file("multi_column.parquet");
        let (array, row_count) = read_row_range(file, 0, 1, 5, 8).unwrap();

        assert_eq!(row_count, 3);
        let str_array = array.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(str_array.value(0), "item_000005");
        assert_eq!(str_array.value(1), "item_000006");
        assert_eq!(str_array.value(2), "item_000007");
    }

    #[test]
    fn test_read_row_range_row_group_1() {
        // Read from row group 1 — data pattern continues with row_idx offset
        // In RG1, id values start at 2000
        let file = get_fixture_file("multi_column.parquet");
        let (array, row_count) = read_row_range(file, 1, 0, 0, 5).unwrap();

        assert_eq!(row_count, 5);
        let int_array = array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        for i in 0..5 {
            assert_eq!(int_array.value(i), (2000 + i) as i32);
        }
    }

    #[test]
    fn test_read_row_range_single_row() {
        // Read exactly one row
        let file = get_fixture_file("multi_column.parquet");
        let (array, row_count) = read_row_range(file, 0, 0, 42, 43).unwrap();

        assert_eq!(row_count, 1);
        assert_eq!(array.len(), 1);
        let int_array = array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 42);
    }

    #[test]
    fn test_read_row_range_invalid_range() {
        let file = get_fixture_file("multi_column.parquet");
        let result = read_row_range(file, 0, 0, 10, 5);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_row_range_out_of_bounds() {
        let file = get_fixture_file("multi_column.parquet");
        let result = read_row_range(file, 0, 0, 1990, 2010);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_row_range_libero_binary() {
        // Read binary (image bytes) column from libero fixture
        let file = get_fixture_file("libero_fixture.parquet");
        let (array, row_count) = read_row_range(file, 0, 0, 0, 5).unwrap();

        assert_eq!(row_count, 5);
        assert_eq!(array.len(), 5);
        // Column 0 is image.bytes (binary)
        let bin_array = array.as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap();
        // Each value should be valid PNG bytes (starts with PNG signature)
        for i in 0..5 {
            let bytes = bin_array.value(i);
            assert!(bytes.len() > 8, "image bytes should be non-trivial");
            assert_eq!(&bytes[..4], b"\x89PNG", "should be valid PNG");
        }
    }

    #[test]
    fn test_read_row_range_libero_list_float() {
        // Read LIST<FLOAT32> element column (state) from libero fixture
        // Column 2 is the state list element column (FLOAT physical type)
        // This reads raw float elements, not reconstructed lists
        let file = get_fixture_file("libero_fixture.parquet");
        let (array, row_count) = read_row_range(file, 0, 2, 0, 10).unwrap();

        // For list element columns, row_count is still end_row - start_row
        assert_eq!(row_count, 10);
        // But array.len() may differ since each "row" in the element column
        // is a single float, while read_rows_from_column slices by element position
        assert!(array.len() > 0);
    }

    #[test]
    fn test_read_row_range_all_rows_in_rg() {
        // Read all 2000 rows from row group 0
        let file = get_fixture_file("multi_column.parquet");
        let (array, row_count) = read_row_range(file, 0, 0, 0, 2000).unwrap();

        assert_eq!(row_count, 2000);
        assert_eq!(array.len(), 2000);

        let int_array = array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 0);
        assert_eq!(int_array.value(1999), 1999);
    }

    // ===================== read_single_row tests =====================

    #[test]
    fn test_read_single_row_first_row_int32() {
        let file = get_fixture_file("multi_column.parquet");
        let array = read_single_row(file, 0, 0, 0).unwrap();
        assert_eq!(array.len(), 1);
        let int_array = array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 0);
    }

    #[test]
    fn test_read_single_row_middle_row_int32() {
        let file = get_fixture_file("multi_column.parquet");
        let array = read_single_row(file, 0, 0, 1000).unwrap();
        assert_eq!(array.len(), 1);
        let int_array = array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 1000);
    }

    #[test]
    fn test_read_single_row_last_row_int32() {
        let file = get_fixture_file("multi_column.parquet");
        let array = read_single_row(file, 0, 0, 1999).unwrap();
        assert_eq!(array.len(), 1);
        let int_array = array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 1999);
    }

    #[test]
    fn test_read_single_row_int64() {
        let file = get_fixture_file("multi_column.parquet");
        // col 2 (score) = row_idx * 100
        let array = read_single_row(file, 0, 2, 42).unwrap();
        assert_eq!(array.len(), 1);
        let int64_array = array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(int64_array.value(0), 42 * 100);
    }

    #[test]
    fn test_read_single_row_string() {
        let file = get_fixture_file("multi_column.parquet");
        // col 1 (name) = "item_{row_idx:06d}"
        let array = read_single_row(file, 0, 1, 123).unwrap();
        assert_eq!(array.len(), 1);
        let str_array = array.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(str_array.value(0), "item_000123");
    }

    #[test]
    fn test_read_single_row_row_group_1() {
        let file = get_fixture_file("multi_column.parquet");
        // Row group 1, row 0 has id = 2000 (offset by RG0's 2000 rows)
        let array = read_single_row(file, 1, 0, 0).unwrap();
        assert_eq!(array.len(), 1);
        let int_array = array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 2000);
    }

    #[test]
    fn test_read_single_row_libero_list_float() {
        let file = get_fixture_file("libero_fixture.parquet");
        // col 2 = state element (FLOAT), read as flat float values
        let array = read_single_row(file, 0, 2, 0).unwrap();
        assert_eq!(array.len(), 1);
        // For list element columns, read_row_range returns raw elements
        let float_array = array.as_any().downcast_ref::<arrow_array::Float32Array>().unwrap();
        assert!(float_array.value(0).is_finite());
    }
}
