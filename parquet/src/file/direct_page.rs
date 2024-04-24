use std::{fs::File, io::Read};
use std::sync::Arc;

use rand::{Rng, thread_rng};
use rand::seq::SliceRandom;

use crate::column::page::{Page, PageReader};
use crate::compression::{Codec, create_codec};
use crate::errors::Result;
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::page_index::index_reader::read_pages_locations;
use crate::file::properties::ReaderProperties;
use crate::file::reader::*;
use crate::file::serialized_reader::decode_page;
use crate::format::{PageHeader, PageLocation};
use crate::thrift::{TCompactSliceInputProtocol, TSerializable};

pub fn get_file_page_locations(
    file: File
) -> Result<Option<Vec<Vec<Vec<PageLocation>>>>> {
    let file_clone = file.try_clone().unwrap();
    let file_reader = SerializedFileReader::new(file_clone).unwrap();
    let num_row_groups = file_reader.num_row_groups();
    let mut file_page_locations = Vec::new();
    for row_group_idx in 0..num_row_groups {
        let row_group_reader = file_reader.get_row_group(row_group_idx)?;
        let page_locations = read_pages_locations(&file, row_group_reader.metadata().columns())?;
        file_page_locations.push(page_locations)
    }

    Ok(Some(file_page_locations)) // 3 dim, row_group x column x page
}

pub fn generate_random_page_indices_file_level(
    file_page_locations: Vec<Vec<Vec<PageLocation>>>,
    column_idx: usize
) -> Result<Vec<(usize, usize)>> {
    let num_row_groups = file_page_locations.len();
    let mut page_num_offsets = Vec::new();
    let mut total_page_num = 0;

    for row_group_idx in 0..num_row_groups{
        page_num_offsets.push(total_page_num);
        total_page_num += file_page_locations[row_group_idx][column_idx].len();
    }

    let mut page_random_indices: Vec<usize> = (0..total_page_num).collect();
    page_random_indices.shuffle(&mut thread_rng());

    // binary search into the page_num_offsets to create (row_group_idx, page_idx) pairs
    let mut random_rg_page_indices_pairs = Vec::new();
    for &page_random_index in &page_random_indices {
        let row_group_idx = page_num_offsets.binary_search(&page_random_index).unwrap_or_else(|idx| idx - 1);
        let page_idx = page_random_index - page_num_offsets[row_group_idx];
        random_rg_page_indices_pairs.push((row_group_idx, page_idx));
    }
    Ok(random_rg_page_indices_pairs) // 2-dim, (row_group_idx, page_idx)
}


pub fn generate_random_page_indices_dataset_level(
    dataset_page_locations: Vec<Vec<Vec<Vec<PageLocation>>>>,
    column_idx: usize
) -> Result<Vec<(usize, usize, usize)>>{
    let num_files = dataset_page_locations.len();
    let mut page_num_offsets_across_file = Vec::new();
    let mut page_num_offsets_within_file = Vec::new();
    let mut total_page_num = 0;
    for file_idx in 0..num_files{
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
        let file_idx = page_num_offsets_across_file.binary_search(&page_random_index).unwrap_or_else(|idx| idx - 1);
        let page_offset_within_file = page_random_index - page_num_offsets_across_file[file_idx];
        let row_group_idx = page_num_offsets_within_file[file_idx].binary_search(&page_offset_within_file).unwrap_or_else(|idx| idx - 1);
        let page_idx = page_offset_within_file - page_num_offsets_within_file[file_idx][row_group_idx];
        random_page_indices.push((file_idx, row_group_idx, page_idx));
    }

    Ok(random_page_indices)

}

pub fn get_page_by_location(
    file: File,
    page_location: PageLocation,
    column_meta: &ColumnChunkMetaData
) -> Result<Option<Page>> {

    // buffer
    let buffer = file.get_bytes(page_location.offset as u64, page_location.compressed_page_size as usize).unwrap();
    let mut prot = TCompactSliceInputProtocol::new(buffer.as_ref());
    let page_header = PageHeader::read_from_in_protocol(&mut prot).unwrap();
    let offset = buffer.len() - prot.as_slice().len();

    let bytes = buffer.slice(offset..);

    // let column_meta = row_group_reader.metadata().column(column_idx);
    let physical_type = column_meta.column_type();
    let props = Arc::new(ReaderProperties::builder().build());
    let decompressor = &mut create_codec(column_meta.compression(), props.codec_options())?;

    let page = decode_page(
        page_header,
        bytes,
        physical_type,
        decompressor.as_mut(),
    )?;

    Ok(Some(page))
}

pub fn get_page_by_idx(
    file: File,
    row_group_idx: usize,
    column_idx: usize,
    page_idx: usize
) -> Result<Option<Page>> {
    let file_clone = file.try_clone().unwrap();
    let file_reader = SerializedFileReader::new(file_clone).unwrap();
    let row_group_reader = file_reader.get_row_group(row_group_idx)?;
    // the iterative page reader for one column
    // let _page_reader = row_group_reader.get_column_page_reader(column_idx).unwrap();

    // Get the page location for the specified column and page index
    let page_locations = read_pages_locations(&file, row_group_reader.metadata().columns())?;
    let page_location = &page_locations[column_idx][page_idx];
    // buffer
    let buffer = file.get_bytes(page_location.offset as u64, page_location.compressed_page_size as usize).unwrap();
    let mut prot = TCompactSliceInputProtocol::new(buffer.as_ref());
    PageHeader::read_from_in_protocol(&mut prot).unwrap();
    let offset = buffer.len() - prot.as_slice().len();

    buffer.slice(offset..);

    let column_meta = row_group_reader.metadata().column(column_idx);
    let page = get_page_by_location(file, page_location.clone(), column_meta);

    page
}

#[cfg(test)]
mod tests {
    use crate::basic::PageType;
    use crate::file::direct_page::{generate_random_page_indices_dataset_level, generate_random_page_indices_file_level, get_file_page_locations, get_page_by_idx};
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::util::test_common::file_util::get_test_file;

    #[test]
    fn test_direct_access_page_by_idx(){
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");

        let page = get_page_by_idx(test_file, 0, 0,0).unwrap().unwrap();

        assert_eq!(page.page_type(), PageType::DATA_PAGE);
    }

    #[test]
    fn test_correctness_of_locations_row_group_num(){
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
    fn test_file_page_num(){
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");
        let page_locations = get_file_page_locations(test_file.try_clone().unwrap()).unwrap().unwrap();
        let page_num = page_locations[0].len();
        println!("the first row group contains {page_num} pages."); // 13
        assert_ne!(page_num, 0);
    }

    #[test]
    fn test_correctness_of_locations_array_shuffle(){
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");
        let file_locations = get_file_page_locations(test_file.try_clone().unwrap()).unwrap().unwrap();
        let column_idx = 0;
        let shuffled_indices = generate_random_page_indices_file_level(
            file_locations,
            column_idx
        ).unwrap();

        let page = get_page_by_idx(
            test_file,
            shuffled_indices[0].0,
            column_idx,
            shuffled_indices[0].1
        ).unwrap().unwrap();

        assert_eq!(page.page_type(), PageType::DATA_PAGE);
    }

    #[test]
    fn test_correctness_dataset_locations_shuffle(){
        let test_file_0 = get_test_file("alltypes_tiny_pages_plain.parquet");
        let test_file_1 = get_test_file("alltypes_tiny_pages_plain.parquet");
        let test_file_2 = get_test_file("alltypes_tiny_pages_plain.parquet");

        let file_locations_0 = get_file_page_locations(test_file_0.try_clone().unwrap()).unwrap().unwrap();
        let file_locations_1 = get_file_page_locations(test_file_1.try_clone().unwrap()).unwrap().unwrap();
        let file_locations_2 = get_file_page_locations(test_file_2.try_clone().unwrap()).unwrap().unwrap();

        let mut dataset_locations = Vec::new();
        dataset_locations.push(file_locations_0);
        dataset_locations.push(file_locations_1);
        dataset_locations.push(file_locations_2);

        let column_idx = 0;

        let random_indices = generate_random_page_indices_dataset_level(dataset_locations,column_idx).unwrap();

        let file_to_read = match random_indices[0].0 {
            0 => test_file_0,
            1 => test_file_1,
            2 => test_file_2,
            _ => panic!()
        };

        let page = get_page_by_idx(file_to_read, random_indices[0].1, column_idx, random_indices[0].2).unwrap().unwrap();
        assert_eq!(page.page_type(), PageType::DATA_PAGE);
    }
}