# read_parquet_metadata

    Code
      mtd$file_meta_data
    Output
      $version
      [1] 1
      
      $num_rows
      [1] 32
      
      $key_value_metadata
      [1] key   value
      <0 rows> (or 0-length row.names)
      
      $created_by
      [1] "https://github.com/gaborcsardi/miniparquet"
      
      $encryption_algorithm
      NULL
      
      $footer_signing_key_metadata
      NULL
      
    Code
      as.data.frame(mtd$schema)
    Output
           name       type type_length repetition_type converted_type logical_type
      1  schema       <NA>          NA            <NA>           <NA>             
      2     nam BYTE_ARRAY          NA        REQUIRED           UTF8       STRING
      3     mpg     DOUBLE          NA        REQUIRED           <NA>             
      4     cyl      INT32          NA        REQUIRED         INT_32 INT, 32,....
      5    disp     DOUBLE          NA        REQUIRED           <NA>             
      6      hp     DOUBLE          NA        REQUIRED           <NA>             
      7    drat     DOUBLE          NA        REQUIRED           <NA>             
      8      wt     DOUBLE          NA        REQUIRED           <NA>             
      9    qsec     DOUBLE          NA        REQUIRED           <NA>             
      10     vs     DOUBLE          NA        REQUIRED           <NA>             
      11     am     DOUBLE          NA        REQUIRED           <NA>             
      12   gear     DOUBLE          NA        REQUIRED           <NA>             
      13   carb     DOUBLE          NA        REQUIRED           <NA>             
      14  large    BOOLEAN          NA        REQUIRED           <NA>             
         num_children scale precision field_id
      1            13    NA        NA       NA
      2            NA    NA        NA       NA
      3            NA    NA        NA       NA
      4            NA    NA        NA       NA
      5            NA    NA        NA       NA
      6            NA    NA        NA       NA
      7            NA    NA        NA       NA
      8            NA    NA        NA       NA
      9            NA    NA        NA       NA
      10           NA    NA        NA       NA
      11           NA    NA        NA       NA
      12           NA    NA        NA       NA
      13           NA    NA        NA       NA
      14           NA    NA        NA       NA
    Code
      as.data.frame(mtd$row_groups)
    Output
        id total_byte_size num_rows file_offset total_compressed_size ordinal
      1  0            3446       32          NA                    NA      NA
    Code
      as.data.frame(mtd$column_chunks)
    Output
         row_group column file_path file_offset offset_index_offset
      1          0      0      <NA>           4                  NA
      2          0      1      <NA>         532                  NA
      3          0      2      <NA>         807                  NA
      4          0      3      <NA>         954                  NA
      5          0      4      <NA>        1229                  NA
      6          0      5      <NA>        1504                  NA
      7          0      6      <NA>        1779                  NA
      8          0      7      <NA>        2054                  NA
      9          0      8      <NA>        2329                  NA
      10         0      9      <NA>        2604                  NA
      11         0     10      <NA>        2879                  NA
      12         0     11      <NA>        3154                  NA
      13         0     12      <NA>        3429                  NA
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

