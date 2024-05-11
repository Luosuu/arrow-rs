// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::util::pretty::print_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::Result;
use std::fs::File;
use parquet::arrow::ProjectionMask;

fn main() -> Result<()> {
    // Create parquet file that will be read.
    let testdata = arrow::util::test_util::parquet_test_data();
    // let path = format!("{testdata}/alltypes_plain.parquet");
    let path = format!("{testdata}/data-pq-00000-int32.snappy.parquet");
    let file = File::open(path).unwrap();

    // Create a sync parquet reader with batch_size.
    // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [0, 2]);
    // let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
    //     .with_batch_size(8192)
    //     .with_projection(0)
    //     .build()?;
    let parquet_reader = builder
        .with_projection(mask)
        .with_batch_size(1024)
        .build()
        .unwrap();

    let mut batches = Vec::new();

    use std::time::Instant;
    let now = Instant::now();
    for batch in parquet_reader {
        batches.push(batch?);
    }
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    // print_batches(&batches).unwrap();
    Ok(())
}
