# read_parquet_row_group

    Code
      as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1]
    Output
        id total_byte_size num_rows file_offset total_compressed_size ordinal
      1  0             937       10          NA                    NA      NA
      2  1             947       10          NA                    NA      NA
      3  2            1011       10          NA                    NA      NA
      4  3             528        2          NA                    NA      NA

---

    Code
      as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1]
    Output
        id total_byte_size num_rows file_offset total_compressed_size ordinal
      1  0             992       10          NA                    NA      NA
      2  1            1029       10          NA                    NA      NA
      3  2            1091       10          NA                    NA      NA
      4  3             606        2          NA                    NA      NA

---

    Code
      as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1]
    Output
        id total_byte_size num_rows file_offset total_compressed_size ordinal
      1  0             937       10          NA                    NA      NA
      2  1             947       10          NA                    NA      NA
      3  2            1011       10          NA                    NA      NA
      4  3             528        2          NA                    NA      NA

---

    Code
      as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1]
    Output
        id total_byte_size num_rows file_offset total_compressed_size ordinal
      1  0             992       10          NA                    NA      NA
      2  1            1029       10          NA                    NA      NA
      3  2            1091       10          NA                    NA      NA
      4  3             606        2          NA                    NA      NA

