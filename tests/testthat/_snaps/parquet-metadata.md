# parquet_metadata

    Code
      as.data.frame(mtd$file_meta_data)
    Output
           file_name version num_rows mtd$file_meta_data$key_value_metadata
      1 test.parquet       1       32                          ARROW:sc....
                                        created_by
      1 https://github.com/gaborcsardi/nanoparquet
    Code
      as.data.frame(mtd$schema)
    Output
            file_name r_col   name    r_type       type type_length repetition_type
      1  test.parquet    NA schema      <NA>       <NA>          NA            <NA>
      2  test.parquet     1    nam character BYTE_ARRAY          NA        REQUIRED
      3  test.parquet     2    mpg    double     DOUBLE          NA        REQUIRED
      4  test.parquet     3    cyl   integer      INT32          NA        REQUIRED
      5  test.parquet     4   disp    double     DOUBLE          NA        REQUIRED
      6  test.parquet     5     hp    double     DOUBLE          NA        REQUIRED
      7  test.parquet     6   drat    double     DOUBLE          NA        REQUIRED
      8  test.parquet     7     wt    double     DOUBLE          NA        REQUIRED
      9  test.parquet     8   qsec    double     DOUBLE          NA        REQUIRED
      10 test.parquet     9     vs    double     DOUBLE          NA        REQUIRED
      11 test.parquet    10     am    double     DOUBLE          NA        REQUIRED
      12 test.parquet    11   gear    double     DOUBLE          NA        REQUIRED
      13 test.parquet    12   carb    double     DOUBLE          NA        REQUIRED
      14 test.parquet    13  large   logical    BOOLEAN          NA        REQUIRED
         converted_type logical_type num_children scale precision field_id children
      1            <NA>                        13    NA        NA       NA         
      2            UTF8       STRING           NA    NA        NA       NA         
      3            <NA>                        NA    NA        NA       NA         
      4          INT_32 INT, 32,....           NA    NA        NA       NA         
      5            <NA>                        NA    NA        NA       NA         
      6            <NA>                        NA    NA        NA       NA         
      7            <NA>                        NA    NA        NA       NA         
      8            <NA>                        NA    NA        NA       NA         
      9            <NA>                        NA    NA        NA       NA         
      10           <NA>                        NA    NA        NA       NA         
      11           <NA>                        NA    NA        NA       NA         
      12           <NA>                        NA    NA        NA       NA         
      13           <NA>                        NA    NA        NA       NA         
      14           <NA>                        NA    NA        NA       NA         
    Code
      as.data.frame(mtd$row_groups)
    Output
           file_name id total_byte_size num_rows file_offset total_compressed_size
      1 test.parquet  0            3446       32          NA                    NA
        ordinal
      1      NA
    Code
      as.data.frame(mtd$column_chunks[, 1:20])
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
         dictionary_page_offset null_count
      1                      NA          0
      2                      NA          0
      3                      NA          0
      4                      NA          0
      5                      NA          0
      6                      NA          0
      7                      NA          0
      8                      NA          0
      9                      NA          0
      10                     NA          0
      11                     NA          0
      12                     NA          0
      13                     NA          0

---

    Code
      as.data.frame(sch)
    Output
            file_name r_col   name    r_type       type type_length repetition_type
      1  test.parquet    NA schema      <NA>       <NA>          NA            <NA>
      2  test.parquet     1    nam character BYTE_ARRAY          NA        REQUIRED
      3  test.parquet     2    mpg    double     DOUBLE          NA        REQUIRED
      4  test.parquet     3    cyl   integer      INT32          NA        REQUIRED
      5  test.parquet     4   disp    double     DOUBLE          NA        REQUIRED
      6  test.parquet     5     hp    double     DOUBLE          NA        REQUIRED
      7  test.parquet     6   drat    double     DOUBLE          NA        REQUIRED
      8  test.parquet     7     wt    double     DOUBLE          NA        REQUIRED
      9  test.parquet     8   qsec    double     DOUBLE          NA        REQUIRED
      10 test.parquet     9     vs    double     DOUBLE          NA        REQUIRED
      11 test.parquet    10     am    double     DOUBLE          NA        REQUIRED
      12 test.parquet    11   gear    double     DOUBLE          NA        REQUIRED
      13 test.parquet    12   carb    double     DOUBLE          NA        REQUIRED
      14 test.parquet    13  large   logical    BOOLEAN          NA        REQUIRED
         converted_type logical_type num_children scale precision field_id children
      1            <NA>                        13    NA        NA       NA         
      2            UTF8       STRING           NA    NA        NA       NA         
      3            <NA>                        NA    NA        NA       NA         
      4          INT_32 INT, 32,....           NA    NA        NA       NA         
      5            <NA>                        NA    NA        NA       NA         
      6            <NA>                        NA    NA        NA       NA         
      7            <NA>                        NA    NA        NA       NA         
      8            <NA>                        NA    NA        NA       NA         
      9            <NA>                        NA    NA        NA       NA         
      10           <NA>                        NA    NA        NA       NA         
      11           <NA>                        NA    NA        NA       NA         
      12           <NA>                        NA    NA        NA       NA         
      13           <NA>                        NA    NA        NA       NA         
      14           <NA>                        NA    NA        NA       NA         

# ENUM type

    Code
      as.data.frame(sch)
    Output
                 file_name r_col                   name
      1  data/enum.parquet    NA             trace.Span
      2  data/enum.parquet     1                     id
      3  data/enum.parquet     2              parent_id
      4  data/enum.parquet     3               trace_id
      5  data/enum.parquet     4                   name
      6  data/enum.parquet     5 start_timestamp_micros
      7  data/enum.parquet     6        duration_micros
      8  data/enum.parquet     7                   tags
      9  data/enum.parquet     7                    key
      10 data/enum.parquet     7                 v_type
      11 data/enum.parquet     7                  v_str
      12 data/enum.parquet     7                 v_bool
      13 data/enum.parquet     7                v_int64
      14 data/enum.parquet     7              v_float64
      15 data/enum.parquet     7               v_binary
                                                                            r_type
      1                                                                       <NA>
      2                                                                        raw
      3                                                                        raw
      4                                                                        raw
      5                                                                  character
      6                                                                     double
      7                                                                     double
      8  list(list(character, character, character, logical, double, double, raw))
      9                                                                       <NA>
      10                                                                      <NA>
      11                                                                      <NA>
      12                                                                      <NA>
      13                                                                      <NA>
      14                                                                      <NA>
      15                                                                      <NA>
               type type_length repetition_type converted_type logical_type
      1        <NA>          NA            <NA>           <NA>             
      2  BYTE_ARRAY          NA        OPTIONAL           <NA>             
      3  BYTE_ARRAY          NA        OPTIONAL           <NA>             
      4  BYTE_ARRAY          NA        OPTIONAL           <NA>             
      5  BYTE_ARRAY          NA        OPTIONAL           UTF8       STRING
      6       INT64          NA        OPTIONAL           <NA>             
      7       INT64          NA        OPTIONAL           <NA>             
      8        <NA>          NA        REPEATED           <NA>             
      9  BYTE_ARRAY          NA        OPTIONAL           UTF8       STRING
      10 BYTE_ARRAY          NA        OPTIONAL           ENUM         ENUM
      11 BYTE_ARRAY          NA        OPTIONAL           UTF8       STRING
      12    BOOLEAN          NA        OPTIONAL           <NA>             
      13      INT64          NA        OPTIONAL           <NA>             
      14     DOUBLE          NA        OPTIONAL           <NA>             
      15 BYTE_ARRAY          NA        OPTIONAL           <NA>             
         num_children scale precision field_id                  children
      1             7    NA        NA       NA                          
      2            NA    NA        NA        1                          
      3            NA    NA        NA        2                          
      4            NA    NA        NA        3                          
      5            NA    NA        NA        4                          
      6            NA    NA        NA        5                          
      7            NA    NA        NA        6                          
      8             7    NA        NA        7 9, 10, 11, 12, 13, 14, 15
      9            NA    NA        NA        1                          
      10           NA    NA        NA        2                          
      11           NA    NA        NA        3                          
      12           NA    NA        NA        4                          
      13           NA    NA        NA        5                          
      14           NA    NA        NA        6                          
      15           NA    NA        NA        7                          

# UUID type

    Code
      as.data.frame(sch)
    Output
                      file_name r_col          name    r_type                 type
      1 data/uuid-arrow.parquet    NA duckdb_schema      <NA>                 <NA>
      2 data/uuid-arrow.parquet     1             u character FIXED_LEN_BYTE_ARRAY
        type_length repetition_type converted_type logical_type num_children scale
      1          NA        REQUIRED           <NA>                         1    NA
      2          16        OPTIONAL           <NA>         UUID            0    NA
        precision field_id children
      1        NA       NA         
      2        NA       NA         

# DATE type

    Code
      as.data.frame(sch)
    Output
                file_name r_col  name r_type  type type_length repetition_type
      1 data/date.parquet    NA dates   <NA>  <NA>          NA            <NA>
      2 data/date.parquet     1     d   Date INT32          NA        OPTIONAL
        converted_type logical_type num_children scale precision field_id children
      1           <NA>                         1    NA        NA       NA         
      2           DATE         DATE           NA    NA        NA       NA         

# DECIMAL type

    Code
      as.data.frame(sch)
    Output
                    file_name r_col   name r_type                 type type_length
      1 data/decimals.parquet    NA schema   <NA>                 <NA>          NA
      2 data/decimals.parquet     1     l1 double FIXED_LEN_BYTE_ARRAY           2
      3 data/decimals.parquet     2     l2 double FIXED_LEN_BYTE_ARRAY           4
      4 data/decimals.parquet     3     l3 double FIXED_LEN_BYTE_ARRAY           7
      5 data/decimals.parquet     4     l4 double FIXED_LEN_BYTE_ARRAY          13
        repetition_type converted_type logical_type num_children scale precision
      1        REQUIRED           <NA>                         4    NA        NA
      2        OPTIONAL        DECIMAL DECIMAL,....           NA     2         3
      3        OPTIONAL        DECIMAL DECIMAL,....           NA     2         8
      4        OPTIONAL        DECIMAL DECIMAL,....           NA     2        15
      5        OPTIONAL        DECIMAL DECIMAL,....           NA     2        30
        field_id children
      1       NA         
      2       NA         
      3       NA         
      4       NA         
      5       NA         

---

    Code
      sch$logical_type
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "DECIMAL"
      
      $scale
      [1] 2
      
      $precision
      [1] 3
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      [[3]]
      $type
      [1] "DECIMAL"
      
      $scale
      [1] 2
      
      $precision
      [1] 8
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      [[4]]
      $type
      [1] "DECIMAL"
      
      $scale
      [1] 2
      
      $precision
      [1] 15
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      [[5]]
      $type
      [1] "DECIMAL"
      
      $scale
      [1] 2
      
      $precision
      [1] 30
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      

# TIME type

    Code
      as.data.frame(sch)
    Output
                  file_name r_col          name r_type  type type_length
      1 data/timetz.parquet    NA duckdb_schema   <NA>  <NA>          NA
      2 data/timetz.parquet     1            tt    hms INT64          NA
        repetition_type converted_type logical_type num_children scale precision
      1        REQUIRED           <NA>                         1    NA        NA
      2        OPTIONAL    TIME_MICROS TIME, TR....           NA    NA        NA
        field_id children
      1       NA         
      2       NA         

---

    Code
      sch$logical_type
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "TIME"
      
      $is_adjusted_to_utc
      [1] TRUE
      
      $unit
      [1] "MICROS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      

# TIMESTAMP type

    Code
      as.data.frame(sch)
    Output
                     file_name r_col   name  r_type  type type_length repetition_type
      1 data/timestamp.parquet    NA schema    <NA>  <NA>          NA        REQUIRED
      2 data/timestamp.parquet     1   Time POSIXct INT64          NA        OPTIONAL
          converted_type logical_type num_children scale precision field_id children
      1             <NA>                         1    NA        NA       NA         
      2 TIMESTAMP_MICROS TIMESTAM....           NA    NA        NA       NA         

---

    Code
      sch$logical_type
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "TIMESTAMP"
      
      $is_adjusted_to_utc
      [1] FALSE
      
      $unit
      [1] "MICROS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      

# LIST type

    Code
      as.data.frame(sch)
    Output
                               file_name r_col         name
      1 data/nested_lists.snappy.parquet    NA spark_schema
      2 data/nested_lists.snappy.parquet     1            a
      3 data/nested_lists.snappy.parquet     1         list
      4 data/nested_lists.snappy.parquet     1      element
      5 data/nested_lists.snappy.parquet     1         list
      6 data/nested_lists.snappy.parquet     1      element
      7 data/nested_lists.snappy.parquet     1         list
      8 data/nested_lists.snappy.parquet     1      element
      9 data/nested_lists.snappy.parquet     2            b
                             r_type       type type_length repetition_type
      1                        <NA>       <NA>          NA            <NA>
      2 list(list(list(character)))       <NA>          NA        OPTIONAL
      3                        <NA>       <NA>          NA        REPEATED
      4                        <NA>       <NA>          NA        OPTIONAL
      5                        <NA>       <NA>          NA        REPEATED
      6                        <NA>       <NA>          NA        OPTIONAL
      7                        <NA>       <NA>          NA        REPEATED
      8                        <NA> BYTE_ARRAY          NA        OPTIONAL
      9                     integer      INT32          NA        REQUIRED
        converted_type logical_type num_children scale precision field_id children
      1           <NA>                         2    NA        NA       NA         
      2           LIST                         1    NA        NA       NA        3
      3           <NA>                         1    NA        NA       NA        4
      4           LIST                         1    NA        NA       NA        5
      5           <NA>                         1    NA        NA       NA        6
      6           LIST                         1    NA        NA       NA        7
      7           <NA>                         1    NA        NA       NA        8
      8           UTF8                        NA    NA        NA       NA         
      9           <NA>                        NA    NA        NA       NA         

# MAP type

    Code
      as.data.frame(sch)
    Output
               file_name r_col        name                                 r_type
      1 data/map.parquet    NA hive_schema                                   <NA>
      2 data/map.parquet     1  raw_header list(list(list(character, character)))
      3 data/map.parquet     1         map                                   <NA>
      4 data/map.parquet     1         key                                   <NA>
      5 data/map.parquet     1       value                                   <NA>
              type type_length repetition_type converted_type logical_type
      1       <NA>          NA            <NA>           <NA>             
      2       <NA>          NA        OPTIONAL            MAP             
      3       <NA>          NA        REPEATED  MAP_KEY_VALUE             
      4 BYTE_ARRAY          NA        REQUIRED           UTF8             
      5 BYTE_ARRAY          NA        OPTIONAL           UTF8             
        num_children scale precision field_id children
      1            1    NA        NA       NA         
      2            1    NA        NA       NA        3
      3            2    NA        NA       NA     4, 5
      4           NA    NA        NA       NA         
      5           NA    NA        NA       NA         

# key-value metadata

    Code
      as.data.frame(mtd$file_meta_data$key_value_metadata[[1]])
    Output
                 key
      1            r
      2 ARROW:schema
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               value
      1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      A\n3\n263168\n197888\n5\nUTF-8\n531\n1\n531\n13\n254\n254\n254\n254\n254\n254\n254\n254\n254\n254\n254\n254\n254\n1026\n1\n262153\n5\nnames\n16\n13\n262153\n4\nname\n262153\n3\nmpg\n262153\n3\ncyl\n262153\n4\ndisp\n262153\n2\nhp\n262153\n4\ndrat\n262153\n2\nwt\n262153\n4\nqsec\n262153\n2\nvs\n262153\n2\nam\n262153\n4\ngear\n262153\n4\ncarb\n262153\n3\nfac\n254\n1026\n511\n16\n1\n262153\n7\ncolumns\n254\n
      2 /////1AEAAAQAAAAAAAKAA4ABgAFAAgACgAAAAABBAAQAAAAAAAKAAwAAAAEAAgACgAAAHwBAAAEAAAAAQAAAAwAAAAIAAwABAAIAAgAAABYAQAABAAAAEkBAABBCjMKMjYzMTY4CjE5Nzg4OAo1ClVURi04CjUzMQoxCjUzMQoxMwoyNTQKMjU0CjI1NAoyNTQKMjU0CjI1NAoyNTQKMjU0CjI1NAoyNTQKMjU0CjI1NAoyNTQKMTAyNgoxCjI2MjE1Mwo1Cm5hbWVzCjE2CjEzCjI2MjE1Mwo0Cm5hbWUKMjYyMTUzCjMKbXBnCjI2MjE1MwozCmN5bAoyNjIxNTMKNApkaXNwCjI2MjE1MwoyCmhwCjI2MjE1Mwo0CmRyYXQKMjYyMTUzCjIKd3QKMjYyMTUzCjQKcXNlYwoyNjIxNTMKMgp2cwoyNjIxNTMKMgphbQoyNjIxNTMKNApnZWFyCjI2MjE1Mwo0CmNhcmIKMjYyMTUzCjMKZmFjCjI1NAoxMDI2CjUxMQoxNgoxCjI2MjE1Mwo3CmNvbHVtbnMKMjU0CgAAAAEAAAByAAAADQAAAHQCAAAwAgAABAIAANQBAACoAQAAeAEAAEwBAAAcAQAA8AAAAMQAAACUAAAAZAAAABQAAAAQABgACAAGAAcADAAQABQAEAAAAAAAAQUUAAAAPAAAABwAAAAEAAAAAAAAAAMAAABmYWMACAAIAAAABAAIAAAADAAAAAgADAAIAAcACAAAAAAAAAEIAAAA9P3//yz+//8AAAEDEAAAABgAAAAEAAAAAAAAAAQAAABjYXJiAAAAAF7+//8AAAIAWP7//wAAAQMQAAAAGAAAAAQAAAAAAAAABAAAAGdlYXIAAAAAiv7//wAAAgCE/v//AAABAxAAAAAUAAAABAAAAAAAAAACAAAAYW0AALL+//8AAAIArP7//wAAAQMQAAAAFAAAAAQAAAAAAAAAAgAAAHZzAADa/v//AAACANT+//8AAAEDEAAAABgAAAAEAAAAAAAAAAQAAABxc2VjAAAAAAb///8AAAIAAP///wAAAQMQAAAAFAAAAAQAAAAAAAAAAgAAAHd0AAAu////AAACACj///8AAAEDEAAAABgAAAAEAAAAAAAAAAQAAABkcmF0AAAAAFr///8AAAIAVP///wAAAQMQAAAAFAAAAAQAAAAAAAAAAgAAAGhwAACC////AAACAHz///8AAAEDEAAAABgAAAAEAAAAAAAAAAQAAABkaXNwAAAAAK7///8AAAIAqP///wAAAQMQAAAAFAAAAAQAAAAAAAAAAwAAAGN5bADW////AAACAND///8AAAEDEAAAABwAAAAEAAAAAAAAAAMAAABtcGcAAAAGAAgABgAGAAAAAAACABAAFAAIAAYABwAMAAAAEAAQAAAAAAABBRAAAAAcAAAABAAAAAAAAAAEAAAAbmFtZQAAAAAEAAQABAAAAAAAAAA=

# parquet_info

    Code
      read_parquet_info(test_path("data/enum.parquet"))
    Output
      # A data frame: 1 x 7
        file_name         num_cols num_rows num_row_groups file_size parquet_version
        <chr>                <int>    <dbl>          <int>     <dbl>           <int>
      1 data/enum.parquet        7        2              1      3930               1
      # i 1 more variable: created_by <chr>
    Code
      read_parquet_info(test_path("data/factor.parquet"))
    Output
      # A data frame: 1 x 7
        file_name           num_cols num_rows num_row_groups file_size parquet_version
        <chr>                  <int>    <dbl>          <int>     <dbl>           <int>
      1 data/factor.parquet       14       32              1      7469               2
      # i 1 more variable: created_by <chr>
    Code
      read_parquet_info(test_path("data/decimals.parquet"))
    Output
      # A data frame: 1 x 7
        file_name           num_cols num_rows num_row_groups file_size parquet_version
        <chr>                  <int>    <dbl>          <int>     <dbl>           <int>
      1 data/decimals.parq~        4        2              1      1653               1
      # i 1 more variable: created_by <chr>

