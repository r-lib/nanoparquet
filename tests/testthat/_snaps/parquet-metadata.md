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

---

    Code
      as.data.frame(sch)
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

# ENUM type

    Code
      as.data.frame(sch)
    Output
                 file_name                   name       type type_length
      1  data/enum.parquet             trace.Span       <NA>          NA
      2  data/enum.parquet                     id BYTE_ARRAY          NA
      3  data/enum.parquet              parent_id BYTE_ARRAY          NA
      4  data/enum.parquet               trace_id BYTE_ARRAY          NA
      5  data/enum.parquet                   name BYTE_ARRAY          NA
      6  data/enum.parquet start_timestamp_micros      INT64          NA
      7  data/enum.parquet        duration_micros      INT64          NA
      8  data/enum.parquet                   tags       <NA>          NA
      9  data/enum.parquet                    key BYTE_ARRAY          NA
      10 data/enum.parquet                 v_type BYTE_ARRAY          NA
      11 data/enum.parquet                  v_str BYTE_ARRAY          NA
      12 data/enum.parquet                 v_bool    BOOLEAN          NA
      13 data/enum.parquet                v_int64      INT64          NA
      14 data/enum.parquet              v_float64     DOUBLE          NA
      15 data/enum.parquet               v_binary BYTE_ARRAY          NA
         repetition_type converted_type logical_type num_children scale precision
      1             <NA>           <NA>                         7    NA        NA
      2         OPTIONAL           <NA>                        NA    NA        NA
      3         OPTIONAL           <NA>                        NA    NA        NA
      4         OPTIONAL           <NA>                        NA    NA        NA
      5         OPTIONAL           UTF8       STRING           NA    NA        NA
      6         OPTIONAL           <NA>                        NA    NA        NA
      7         OPTIONAL           <NA>                        NA    NA        NA
      8         REPEATED           <NA>                         7    NA        NA
      9         OPTIONAL           UTF8       STRING           NA    NA        NA
      10        OPTIONAL           ENUM         ENUM           NA    NA        NA
      11        OPTIONAL           UTF8       STRING           NA    NA        NA
      12        OPTIONAL           <NA>                        NA    NA        NA
      13        OPTIONAL           <NA>                        NA    NA        NA
      14        OPTIONAL           <NA>                        NA    NA        NA
      15        OPTIONAL           <NA>                        NA    NA        NA
         field_id
      1        NA
      2         1
      3         2
      4         3
      5         4
      6         5
      7         6
      8         7
      9         1
      10        2
      11        3
      12        4
      13        5
      14        6
      15        7

# UUID type

    Code
      as.data.frame(sch)
    Output
                      file_name          name                 type type_length
      1 data/uuid-arrow.parquet duckdb_schema                 <NA>          NA
      2 data/uuid-arrow.parquet             u FIXED_LEN_BYTE_ARRAY          16
        repetition_type converted_type logical_type num_children scale precision
      1        REQUIRED           <NA>                         1    NA        NA
      2        OPTIONAL           <NA>         UUID            0    NA        NA
        field_id
      1       NA
      2       NA

# DATE type

    Code
      as.data.frame(sch)
    Output
                file_name  name  type type_length repetition_type converted_type
      1 data/date.parquet dates  <NA>          NA            <NA>           <NA>
      2 data/date.parquet     d INT32          NA        OPTIONAL           DATE
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2         DATE           NA    NA        NA       NA

# DECIMAL type

    Code
      as.data.frame(sch)
    Output
                    file_name   name                 type type_length repetition_type
      1 data/decimals.parquet schema                 <NA>          NA        REQUIRED
      2 data/decimals.parquet     l1 FIXED_LEN_BYTE_ARRAY           2        OPTIONAL
      3 data/decimals.parquet     l2 FIXED_LEN_BYTE_ARRAY           4        OPTIONAL
      4 data/decimals.parquet     l3 FIXED_LEN_BYTE_ARRAY           7        OPTIONAL
      5 data/decimals.parquet     l4 FIXED_LEN_BYTE_ARRAY          13        OPTIONAL
        converted_type logical_type num_children scale precision field_id
      1           <NA>                         4    NA        NA       NA
      2        DECIMAL DECIMAL,....           NA     2         3       NA
      3        DECIMAL DECIMAL,....           NA     2         8       NA
      4        DECIMAL DECIMAL,....           NA     2        15       NA
      5        DECIMAL DECIMAL,....           NA     2        30       NA

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
                  file_name          name  type type_length repetition_type
      1 data/timetz.parquet duckdb_schema  <NA>          NA        REQUIRED
      2 data/timetz.parquet            tt INT64          NA        OPTIONAL
        converted_type logical_type num_children scale precision field_id
      1           <NA>                         1    NA        NA       NA
      2    TIME_MICROS TIME, TR....           NA    NA        NA       NA

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
      [1] "micros"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      

# TIMESTAMP type

    Code
      as.data.frame(sch)
    Output
                     file_name   name  type type_length repetition_type
      1 data/timestamp.parquet schema  <NA>          NA        REQUIRED
      2 data/timestamp.parquet   Time INT64          NA        OPTIONAL
          converted_type logical_type num_children scale precision field_id
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
      [1] "micros"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      

# LIST type

    Code
      as.data.frame(sch)
    Output
                               file_name         name       type type_length
      1 data/nested_lists.snappy.parquet spark_schema       <NA>          NA
      2 data/nested_lists.snappy.parquet            a       <NA>          NA
      3 data/nested_lists.snappy.parquet         list       <NA>          NA
      4 data/nested_lists.snappy.parquet      element       <NA>          NA
      5 data/nested_lists.snappy.parquet         list       <NA>          NA
      6 data/nested_lists.snappy.parquet      element       <NA>          NA
      7 data/nested_lists.snappy.parquet         list       <NA>          NA
      8 data/nested_lists.snappy.parquet      element BYTE_ARRAY          NA
      9 data/nested_lists.snappy.parquet            b      INT32          NA
        repetition_type converted_type logical_type num_children scale precision
      1            <NA>           <NA>                         2    NA        NA
      2        OPTIONAL           LIST                         1    NA        NA
      3        REPEATED           <NA>                         1    NA        NA
      4        OPTIONAL           LIST                         1    NA        NA
      5        REPEATED           <NA>                         1    NA        NA
      6        OPTIONAL           LIST                         1    NA        NA
      7        REPEATED           <NA>                         1    NA        NA
      8        OPTIONAL           UTF8                        NA    NA        NA
      9        REQUIRED           <NA>                        NA    NA        NA
        field_id
      1       NA
      2       NA
      3       NA
      4       NA
      5       NA
      6       NA
      7       NA
      8       NA
      9       NA

# MAP type

    Code
      as.data.frame(sch)
    Output
               file_name        name       type type_length repetition_type
      1 data/map.parquet hive_schema       <NA>          NA            <NA>
      2 data/map.parquet  raw_header       <NA>          NA        OPTIONAL
      3 data/map.parquet         map       <NA>          NA        REPEATED
      4 data/map.parquet         key BYTE_ARRAY          NA        REQUIRED
      5 data/map.parquet       value BYTE_ARRAY          NA        OPTIONAL
        converted_type logical_type num_children scale precision field_id
      1           <NA>                         1    NA        NA       NA
      2            MAP                         1    NA        NA       NA
      3  MAP_KEY_VALUE                         2    NA        NA       NA
      4           UTF8                        NA    NA        NA       NA
      5           UTF8                        NA    NA        NA       NA

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

# parquet_column_types

    Code
      parquet_column_types(test_path("data/enum.parquet"))
    Output
      # A data frame: 13 x 6
         file_name         name              type  r_type repetition_type logical_type
       * <chr>             <chr>             <chr> <chr>  <chr>           <I<list>>   
       1 data/enum.parquet id                BYTE~ raw    OPTIONAL        <NULL>      
       2 data/enum.parquet parent_id         BYTE~ raw    OPTIONAL        <NULL>      
       3 data/enum.parquet trace_id          BYTE~ raw    OPTIONAL        <NULL>      
       4 data/enum.parquet name              BYTE~ chara~ OPTIONAL        <STRING>    
       5 data/enum.parquet start_timestamp_~ INT64 double OPTIONAL        <NULL>      
       6 data/enum.parquet duration_micros   INT64 double OPTIONAL        <NULL>      
       7 data/enum.parquet key               BYTE~ chara~ OPTIONAL        <STRING>    
       8 data/enum.parquet v_type            BYTE~ chara~ OPTIONAL        <ENUM>      
       9 data/enum.parquet v_str             BYTE~ chara~ OPTIONAL        <STRING>    
      10 data/enum.parquet v_bool            BOOL~ logic~ OPTIONAL        <NULL>      
      11 data/enum.parquet v_int64           INT64 double OPTIONAL        <NULL>      
      12 data/enum.parquet v_float64         DOUB~ double OPTIONAL        <NULL>      
      13 data/enum.parquet v_binary          BYTE~ raw    OPTIONAL        <NULL>      
    Code
      parquet_column_types(test_path("data/factor.parquet"))
    Output
      # A data frame: 14 x 6
         file_name           name  type       r_type    repetition_type logical_type
       * <chr>               <chr> <chr>      <chr>     <chr>           <I<list>>   
       1 data/factor.parquet nam   BYTE_ARRAY character OPTIONAL        <STRING>    
       2 data/factor.parquet mpg   DOUBLE     double    OPTIONAL        <NULL>      
       3 data/factor.parquet cyl   INT32      integer   OPTIONAL        <NULL>      
       4 data/factor.parquet disp  DOUBLE     double    OPTIONAL        <NULL>      
       5 data/factor.parquet hp    DOUBLE     double    OPTIONAL        <NULL>      
       6 data/factor.parquet drat  DOUBLE     double    OPTIONAL        <NULL>      
       7 data/factor.parquet wt    DOUBLE     double    OPTIONAL        <NULL>      
       8 data/factor.parquet qsec  DOUBLE     double    OPTIONAL        <NULL>      
       9 data/factor.parquet vs    DOUBLE     double    OPTIONAL        <NULL>      
      10 data/factor.parquet am    DOUBLE     double    OPTIONAL        <NULL>      
      11 data/factor.parquet gear  DOUBLE     double    OPTIONAL        <NULL>      
      12 data/factor.parquet carb  DOUBLE     double    OPTIONAL        <NULL>      
      13 data/factor.parquet large BOOLEAN    logical   OPTIONAL        <NULL>      
      14 data/factor.parquet fac   BYTE_ARRAY factor    OPTIONAL        <STRING>    
    Code
      parquet_column_types(test_path("data/decimals.parquet"))
    Output
      # A data frame: 4 x 6
        file_name             name  type       r_type repetition_type logical_type    
      * <chr>                 <chr> <chr>      <chr>  <chr>           <I<list>>       
      1 data/decimals.parquet l1    FIXED_LEN~ double OPTIONAL        <DECIMAL(2, 3)> 
      2 data/decimals.parquet l2    FIXED_LEN~ double OPTIONAL        <DECIMAL(2, 8)> 
      3 data/decimals.parquet l3    FIXED_LEN~ double OPTIONAL        <DECIMAL(2, 15)>
      4 data/decimals.parquet l4    FIXED_LEN~ double OPTIONAL        <DECIMAL(2, 30)>

---

    Code
      parquet_column_types(tmp)[-1]
    Output
      # A data frame: 4 x 5
        name     type  r_type   repetition_type logical_type             
      * <chr>    <chr> <chr>    <chr>           <I<list>>                
      1 Date     INT32 Date     REQUIRED        <DATE>                   
      2 POSIXct  INT64 POSIXct  REQUIRED        <TIMESTAMP(TRUE, micros)>
      3 hms      INT32 hms      REQUIRED        <TIME(TRUE, millis)>     
      4 difftime INT64 difftime REQUIRED        <NULL>                   
    Code
      parquet_column_types(tmp)$r_type
    Output
      [1] "Date"     "POSIXct"  "hms"      "difftime"

# parquet_info

    Code
      parquet_info(test_path("data/enum.parquet"))
    Output
      # A data frame: 1 x 7
        file_name         num_cols num_rows num_row_groups file_size parquet_version
        <chr>                <int>    <dbl>          <int>     <dbl>           <int>
      1 data/enum.parquet       13        2              1      3930               1
      # i 1 more variable: created_by <chr>
    Code
      parquet_info(test_path("data/factor.parquet"))
    Output
      # A data frame: 1 x 7
        file_name           num_cols num_rows num_row_groups file_size parquet_version
        <chr>                  <int>    <dbl>          <int>     <dbl>           <int>
      1 data/factor.parquet       14       32              1      7469               2
      # i 1 more variable: created_by <chr>
    Code
      parquet_info(test_path("data/decimals.parquet"))
    Output
      # A data frame: 1 x 7
        file_name           num_cols num_rows num_row_groups file_size parquet_version
        <chr>                  <int>    <dbl>          <int>     <dbl>           <int>
      1 data/decimals.parq~        4        2              1      1653               1
      # i 1 more variable: created_by <chr>

