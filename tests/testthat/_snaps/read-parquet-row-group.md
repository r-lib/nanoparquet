# plain and simple

    Code
      as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1]
    Output
        id total_byte_size num_rows file_offset total_compressed_size ordinal
      1  0             889       10          NA                    NA      NA
      2  1             895       10          NA                    NA      NA
      3  2             924       10          NA                    NA      NA
      4  3             447        2          NA                    NA      NA

# dicts

    Code
      as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1]
    Output
        id total_byte_size num_rows file_offset total_compressed_size ordinal
      1  0            1072       10          NA                    NA      NA
      2  1            1111       10          NA                    NA      NA
      3  2            1183       10          NA                    NA      NA
      4  3             659        2          NA                    NA      NA

