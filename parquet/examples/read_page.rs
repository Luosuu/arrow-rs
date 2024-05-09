use std::fs::File;

use parquet::errors::Result;
use parquet::file::direct_page::{generate_random_page_indices_file_level, get_file_page_locations, read_record_from_page};

fn main() -> Result<()> {
    // Create parquet file that will be read.
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/data-pq-00000-int32.parquet");
    let test_file = File::open(path).unwrap();


    let column_idx = 0;

    let page_location_matrix = get_file_page_locations(test_file.try_clone().unwrap())
        .unwrap()
        .unwrap();

    let mut i = 0;
    for column in &page_location_matrix[0] {
        let len = column.len();
        println!("the {:?}-th column that has num of pages: {:?}", i, len);
        i += 1;
    }

    let random_page_indices = generate_random_page_indices_file_level(page_location_matrix, column_idx)
        .unwrap(); // (row_group_index, page_index)

    use std::time::Instant;
    let now = Instant::now();

    for (row_group_index, page_index) in random_page_indices {
        let _array_input_ids = read_record_from_page(
            test_file.try_clone().unwrap(),
            row_group_index,
            0,
            page_index
        )
            .unwrap()
            .unwrap();

        let _array_labels = read_record_from_page(
            test_file.try_clone().unwrap(),
            row_group_index,
            2,
            page_index
        )
            .unwrap()
            .unwrap();
    }

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);


    Ok(())
}