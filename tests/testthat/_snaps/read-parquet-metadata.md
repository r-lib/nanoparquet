# read_parquet_metadata

    Code
      mtd$file_meta_data
    Output
      # A data frame: 1 x 5
        file_name    version num_rows key_value_metadata$mtd$file_meta_da~1 created_by
        <chr>          <int>    <dbl> <I<list>>                             <chr>     
      1 test.parquet       1       32 <tbl [0 x 2]>                         https://g~
      # i abbreviated name:
      #   1: key_value_metadata$`mtd$file_meta_data$key_value_metadata`
    Code
      as.data.frame(mtd$schema)
    Output
            file_name   name       type type_length repetition_type converted_type
      1  test.parquet schema       <NA>          NA            <NA>           <NA>
      2  test.parquet    nam BYTE_ARRAY          NA        REQUIRED           UTF8
      3  test.parquet    mpg     DOUBLE          NA        REQUIRED           <NA>
      4  test.parquet    cyl      INT32          NA        REQUIRED         INT_32
      5  test.parquet   disp     DOUBLE          NA        REQUIRED           <NA>
      6  test.parquet     hp     DOUBLE          NA        REQUIRED           <NA>
      7  test.parquet   drat     DOUBLE          NA        REQUIRED           <NA>
      8  test.parquet     wt     DOUBLE          NA        REQUIRED           <NA>
      9  test.parquet   qsec     DOUBLE          NA        REQUIRED           <NA>
      10 test.parquet     vs     DOUBLE          NA        REQUIRED           <NA>
      11 test.parquet     am     DOUBLE          NA        REQUIRED           <NA>
      12 test.parquet   gear     DOUBLE          NA        REQUIRED           <NA>
      13 test.parquet   carb     DOUBLE          NA        REQUIRED           <NA>
      14 test.parquet  large    BOOLEAN          NA        REQUIRED           <NA>
         logical_type num_children scale precision field_id
      1                         13    NA        NA       NA
      2        STRING           NA    NA        NA       NA
      3                         NA    NA        NA       NA
      4  INT, 32,....           NA    NA        NA       NA
      5                         NA    NA        NA       NA
      6                         NA    NA        NA       NA
      7                         NA    NA        NA       NA
      8                         NA    NA        NA       NA
      9                         NA    NA        NA       NA
      10                        NA    NA        NA       NA
      11                        NA    NA        NA       NA
      12                        NA    NA        NA       NA
      13                        NA    NA        NA       NA
      14                        NA    NA        NA       NA
    Code
      as.data.frame(mtd$row_groups)
    Output
           file_name id total_byte_size num_rows file_offset total_compressed_size
      1 test.parquet  0            3446       32          NA                    NA
        ordinal
      1      NA
    Code
      as.data.frame(mtd$column_chunks)
    Output
            file_name row_group column file_path file_offset offset_index_offset
      1  test.parquet         0      0      <NA>           4                  NA
      2  test.parquet         0      1      <NA>         532                  NA
      3  test.parquet         0      2      <NA>         807                  NA
      4  test.parquet         0      3      <NA>         954                  NA
      5  test.parquet         0      4      <NA>        1229                  NA
      6  test.parquet         0      5      <NA>        1504                  NA
      7  test.parquet         0      6      <NA>        1779                  NA
      8  test.parquet         0      7      <NA>        2054                  NA
      9  test.parquet         0      8      <NA>        2329                  NA
      10 test.parquet         0      9      <NA>        2604                  NA
      11 test.parquet         0     10      <NA>        2879                  NA
      12 test.parquet         0     11      <NA>        3154                  NA
      13 test.parquet         0     12      <NA>        3429                  NA
         offset_index_length column_index_offset column_index_length       type
      1                   NA                  NA                  NA BYTE_ARRAY
      2                   NA                  NA                  NA     DOUBLE
      3                   NA                  NA                  NA      INT32
      4                   NA                  NA                  NA     DOUBLE
      5                   NA                  NA                  NA     DOUBLE
      6                   NA                  NA                  NA     DOUBLE
      7                   NA                  NA                  NA     DOUBLE
      8                   NA                  NA                  NA     DOUBLE
      9                   NA                  NA                  NA     DOUBLE
      10                  NA                  NA                  NA     DOUBLE
      11                  NA                  NA                  NA     DOUBLE
      12                  NA                  NA                  NA     DOUBLE
      13                  NA                  NA                  NA    BOOLEAN
         encodings path_in_schema        codec num_values total_uncompressed_size
      1      PLAIN            nam UNCOMPRESSED         32                     528
      2      PLAIN            mpg UNCOMPRESSED         32                     275
      3      PLAIN            cyl UNCOMPRESSED         32                     147
      4      PLAIN           disp UNCOMPRESSED         32                     275
      5      PLAIN             hp UNCOMPRESSED         32                     275
      6      PLAIN           drat UNCOMPRESSED         32                     275
      7      PLAIN             wt UNCOMPRESSED         32                     275
      8      PLAIN           qsec UNCOMPRESSED         32                     275
      9      PLAIN             vs UNCOMPRESSED         32                     275
      10     PLAIN             am UNCOMPRESSED         32                     275
      11     PLAIN           gear UNCOMPRESSED         32                     275
      12     PLAIN           carb UNCOMPRESSED         32                     275
      13     PLAIN          large UNCOMPRESSED         32                      21
         total_compressed_size data_page_offset index_page_offset
      1                    528                4                NA
      2                    275              532                NA
      3                    147              807                NA
      4                    275              954                NA
      5                    275             1229                NA
      6                    275             1504                NA
      7                    275             1779                NA
      8                    275             2054                NA
      9                    275             2329                NA
      10                   275             2604                NA
      11                   275             2879                NA
      12                   275             3154                NA
      13                    21             3429                NA
         dictionary_page_offset
      1                      NA
      2                      NA
      3                      NA
      4                      NA
      5                      NA
      6                      NA
      7                      NA
      8                      NA
      9                      NA
      10                     NA
      11                     NA
      12                     NA
      13                     NA

