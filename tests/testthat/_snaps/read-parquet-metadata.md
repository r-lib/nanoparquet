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
         row_group file_path file_offset offset_index_offset offset_index_length
      1          0      <NA>           4                  NA                  NA
      2          0      <NA>         532                  NA                  NA
      3          0      <NA>         807                  NA                  NA
      4          0      <NA>         954                  NA                  NA
      5          0      <NA>        1229                  NA                  NA
      6          0      <NA>        1504                  NA                  NA
      7          0      <NA>        1779                  NA                  NA
      8          0      <NA>        2054                  NA                  NA
      9          0      <NA>        2329                  NA                  NA
      10         0      <NA>        2604                  NA                  NA
      11         0      <NA>        2879                  NA                  NA
      12         0      <NA>        3154                  NA                  NA
      13         0      <NA>        3429                  NA                  NA
         column_index_offset column_index_length type encodings path_in_schema
      1                   NA                  NA    6     PLAIN            nam
      2                   NA                  NA    5     PLAIN            mpg
      3                   NA                  NA    1     PLAIN            cyl
      4                   NA                  NA    5     PLAIN           disp
      5                   NA                  NA    5     PLAIN             hp
      6                   NA                  NA    5     PLAIN           drat
      7                   NA                  NA    5     PLAIN             wt
      8                   NA                  NA    5     PLAIN           qsec
      9                   NA                  NA    5     PLAIN             vs
      10                  NA                  NA    5     PLAIN             am
      11                  NA                  NA    5     PLAIN           gear
      12                  NA                  NA    5     PLAIN           carb
      13                  NA                  NA    0     PLAIN          large
                codec num_values total_uncompressed_size total_compressed_size
      1  UNCOMPRESSED         32                     528                   528
      2  UNCOMPRESSED         32                     275                   275
      3  UNCOMPRESSED         32                     147                   147
      4  UNCOMPRESSED         32                     275                   275
      5  UNCOMPRESSED         32                     275                   275
      6  UNCOMPRESSED         32                     275                   275
      7  UNCOMPRESSED         32                     275                   275
      8  UNCOMPRESSED         32                     275                   275
      9  UNCOMPRESSED         32                     275                   275
      10 UNCOMPRESSED         32                     275                   275
      11 UNCOMPRESSED         32                     275                   275
      12 UNCOMPRESSED         32                     275                   275
      13 UNCOMPRESSED         32                      21                    21
         data_page_offset index_page_offset dictionary_page_offset
      1                 4                NA                     NA
      2               532                NA                     NA
      3               807                NA                     NA
      4               954                NA                     NA
      5              1229                NA                     NA
      6              1504                NA                     NA
      7              1779                NA                     NA
      8              2054                NA                     NA
      9              2329                NA                     NA
      10             2604                NA                     NA
      11             2879                NA                     NA
      12             3154                NA                     NA
      13             3429                NA                     NA

