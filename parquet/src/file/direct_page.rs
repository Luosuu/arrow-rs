use std::{fs::File, io::Read};
use std::sync::Arc;

use crate::column::page::{Page, PageReader};
use crate::compression::{Codec, create_codec};
use crate::errors::Result;
use crate::file::page_index::index_reader::read_pages_locations;
use crate::file::properties::ReaderProperties;
use crate::file::reader::*;
use crate::file::serialized_reader::decode_page;
use crate::format::PageHeader;
use crate::thrift::{TCompactSliceInputProtocol, TSerializable};

pub fn get_page(
    file: File,
    row_group_idx: usize,
    column_idx: usize,
    page_idx: usize
) -> Result<Option<Page>> {
    let file_clone = file.try_clone().unwrap();
    let file_reader = SerializedFileReader::new(file_clone).unwrap();
    let row_group_reader = file_reader.get_row_group(row_group_idx)?;
    // the iterative page reader for one column
    let page_reader = row_group_reader.get_column_page_reader(column_idx).unwrap();
    // Get the page location for the specified column and page index
    let page_locations = read_pages_locations(&file, row_group_reader.metadata().columns())?;
    let page_location = &page_locations[column_idx][page_idx];
    // buffer
    let buffer = file.get_bytes(page_location.offset as u64, page_location.compressed_page_size as usize).unwrap();
    let mut prot = TCompactSliceInputProtocol::new(buffer.as_ref());
    let page_header = PageHeader::read_from_in_protocol(&mut prot).unwrap();
    let offset = buffer.len() - prot.as_slice().len();

    let bytes = buffer.slice(offset..);

    let meta = row_group_reader.metadata().column(column_idx);
    let physical_type = meta.column_type();
    let props = Arc::new(ReaderProperties::builder().build());
    let decompressor = &mut create_codec(meta.compression(), props.codec_options())?;

    let page = decode_page(
        page_header,
        bytes,
        physical_type,
        decompressor.as_mut(),
    )?;

    Ok(Some(page))
}

#[cfg(test)]
mod tests {
    use crate::basic::PageType;
    use crate::file::direct_page::get_page;
    use crate::util::test_common::file_util::get_test_file;

    fn test_direct_access_page(){
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");

        let page = get_page(test_file, 0, 0,0).unwrap();

        assert_eq!(page.unwrap().page_type(), PageType::DATA_PAGE_V2);
    }
}