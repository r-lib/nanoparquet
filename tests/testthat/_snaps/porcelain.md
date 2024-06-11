# parquet_pages

    Code
      as.data.frame(pp)
    Output
                         file_name row_group column       page_type
      1  data/mtcars-arrow.parquet         0      0 DICTIONARY_PAGE
      2  data/mtcars-arrow.parquet         0      0       DATA_PAGE
      3  data/mtcars-arrow.parquet         0      1 DICTIONARY_PAGE
      4  data/mtcars-arrow.parquet         0      1       DATA_PAGE
      5  data/mtcars-arrow.parquet         0      2 DICTIONARY_PAGE
      6  data/mtcars-arrow.parquet         0      2       DATA_PAGE
      7  data/mtcars-arrow.parquet         0      3 DICTIONARY_PAGE
      8  data/mtcars-arrow.parquet         0      3       DATA_PAGE
      9  data/mtcars-arrow.parquet         0      4 DICTIONARY_PAGE
      10 data/mtcars-arrow.parquet         0      4       DATA_PAGE
      11 data/mtcars-arrow.parquet         0      5 DICTIONARY_PAGE
      12 data/mtcars-arrow.parquet         0      5       DATA_PAGE
      13 data/mtcars-arrow.parquet         0      6 DICTIONARY_PAGE
      14 data/mtcars-arrow.parquet         0      6       DATA_PAGE
      15 data/mtcars-arrow.parquet         0      7 DICTIONARY_PAGE
      16 data/mtcars-arrow.parquet         0      7       DATA_PAGE
      17 data/mtcars-arrow.parquet         0      8 DICTIONARY_PAGE
      18 data/mtcars-arrow.parquet         0      8       DATA_PAGE
      19 data/mtcars-arrow.parquet         0      9 DICTIONARY_PAGE
      20 data/mtcars-arrow.parquet         0      9       DATA_PAGE
      21 data/mtcars-arrow.parquet         0     10 DICTIONARY_PAGE
      22 data/mtcars-arrow.parquet         0     10       DATA_PAGE
      23 data/mtcars-arrow.parquet         0     11 DICTIONARY_PAGE
      24 data/mtcars-arrow.parquet         0     11       DATA_PAGE
      25 data/mtcars-arrow.parquet         0     12 DICTIONARY_PAGE
      26 data/mtcars-arrow.parquet         0     12       DATA_PAGE
         page_header_offset uncompressed_page_size compressed_page_size crc
      1                   4                    509                  424  NA
      2                 444                     28                   30  NA
      3                 600                    200                  131  NA
      4                 747                     28                   30  NA
      5                 933                     24                   22  NA
      6                 969                     16                   18  NA
      7                1143                    216                  149  NA
      8                1308                     28                   30  NA
      9                1495                    176                  118  NA
      10               1629                     28                   30  NA
      11               1814                    176                  149  NA
      12               1979                     28                   30  NA
      13               2166                    232                  188  NA
      14               2370                     28                   30  NA
      15               2555                    240                  195  NA
      16               2766                     28                   30  NA
      17               2953                     16                   18  NA
      18               2985                     12                   14  NA
      19               3154                     16                   18  NA
      20               3186                     14                   16  NA
      21               3357                     24                   22  NA
      22               3393                     16                   18  NA
      23               3568                     48                   35  NA
      24               3617                     20                   22  NA
      25               3796                     25                   26  NA
      26               3836                     20                   22  NA
         num_values       encoding definition_level_encoding
      1          32          PLAIN                      <NA>
      2          32 RLE_DICTIONARY                       RLE
      3          25          PLAIN                      <NA>
      4          32 RLE_DICTIONARY                       RLE
      5           3          PLAIN                      <NA>
      6          32 RLE_DICTIONARY                       RLE
      7          27          PLAIN                      <NA>
      8          32 RLE_DICTIONARY                       RLE
      9          22          PLAIN                      <NA>
      10         32 RLE_DICTIONARY                       RLE
      11         22          PLAIN                      <NA>
      12         32 RLE_DICTIONARY                       RLE
      13         29          PLAIN                      <NA>
      14         32 RLE_DICTIONARY                       RLE
      15         30          PLAIN                      <NA>
      16         32 RLE_DICTIONARY                       RLE
      17          2          PLAIN                      <NA>
      18         32 RLE_DICTIONARY                       RLE
      19          2          PLAIN                      <NA>
      20         32 RLE_DICTIONARY                       RLE
      21          3          PLAIN                      <NA>
      22         32 RLE_DICTIONARY                       RLE
      23          6          PLAIN                      <NA>
      24         32 RLE_DICTIONARY                       RLE
      25          5          PLAIN                      <NA>
      26         32 RLE_DICTIONARY                       RLE
         repetition_level_encoding data_offset page_header_length
      1                       <NA>          20                 16
      2                        RLE         490                 46
      3                       <NA>         616                 16
      4                        RLE         808                 61
      5                       <NA>         947                 14
      6                        RLE        1030                 61
      7                       <NA>        1159                 16
      8                        RLE        1369                 61
      9                       <NA>        1511                 16
      10                       RLE        1690                 61
      11                      <NA>        1830                 16
      12                       RLE        2040                 61
      13                      <NA>        2182                 16
      14                       RLE        2431                 61
      15                      <NA>        2571                 16
      16                       RLE        2827                 61
      17                      <NA>        2967                 14
      18                       RLE        3046                 61
      19                      <NA>        3168                 14
      20                       RLE        3247                 61
      21                      <NA>        3371                 14
      22                       RLE        3454                 61
      23                      <NA>        3582                 14
      24                       RLE        3678                 61
      25                      <NA>        3810                 14
      26                       RLE        3863                 27

# read_parquet_page

    Code
      page
    Output
      $page_type
      [1] "DICTIONARY_PAGE"
      
      $row_group
      [1] 0
      
      $column
      [1] 0
      
      $page_header_offset
      [1] 4
      
      $data_page_offset
      [1] 20
      
      $page_header_length
      [1] 16
      
      $compressed_page_size
      [1] 424
      
      $uncompressed_page_size
      [1] 509
      
      $codec
      [1] "SNAPPY"
      
      $num_values
      [1] 32
      
      $encoding
      [1] "PLAIN"
      
      $definition_level_encoding
      [1] NA
      
      $repetition_level_encoding
      [1] NA
      
      $has_repetition_levels
      [1] FALSE
      
      $has_definition_levels
      [1] FALSE
      
      $schema_column
      [1] 1
      
      $data_type
      [1] "BYTE_ARRAY"
      
      $repetition_type
      [1] "OPTIONAL"
      
      $page_header
       [1] 15 04 15 fa 07 15 d0 06 4c 15 40 15 00 12 00 00
      
      $data
        [1] 09 00 00 00 4d 61 7a 64 61 20 52 58 34 0d 00 00 00 4d 61 7a 64 61 20 52 58
       [26] 34 20 57 61 67 0a 00 00 00 44 61 74 73 75 6e 20 37 31 30 0e 00 00 00 48 6f
       [51] 72 6e 65 74 20 34 20 44 72 69 76 65 11 00 00 00 48 6f 72 6e 65 74 20 53 70
       [76] 6f 72 74 61 62 6f 75 74 07 00 00 00 56 61 6c 69 61 6e 74 0a 00 00 00 44 75
      [101] 73 74 65 72 20 33 36 30 09 00 00 00 4d 65 72 63 20 32 34 30 44 08 00 00 00
      [126] 4d 65 72 63 20 32 33 30 08 00 00 00 4d 65 72 63 20 32 38 30 09 00 00 00 4d
      [151] 65 72 63 20 32 38 30 43 0a 00 00 00 4d 65 72 63 20 34 35 30 53 45 0a 00 00
      [176] 00 4d 65 72 63 20 34 35 30 53 4c 0b 00 00 00 4d 65 72 63 20 34 35 30 53 4c
      [201] 43 12 00 00 00 43 61 64 69 6c 6c 61 63 20 46 6c 65 65 74 77 6f 6f 64 13 00
      [226] 00 00 4c 69 6e 63 6f 6c 6e 20 43 6f 6e 74 69 6e 65 6e 74 61 6c 11 00 00 00
      [251] 43 68 72 79 73 6c 65 72 20 49 6d 70 65 72 69 61 6c 08 00 00 00 46 69 61 74
      [276] 20 31 32 38 0b 00 00 00 48 6f 6e 64 61 20 43 69 76 69 63 0e 00 00 00 54 6f
      [301] 79 6f 74 61 20 43 6f 72 6f 6c 6c 61 0d 00 00 00 54 6f 79 6f 74 61 20 43 6f
      [326] 72 6f 6e 61 10 00 00 00 44 6f 64 67 65 20 43 68 61 6c 6c 65 6e 67 65 72 0b
      [351] 00 00 00 41 4d 43 20 4a 61 76 65 6c 69 6e 0a 00 00 00 43 61 6d 61 72 6f 20
      [376] 5a 32 38 10 00 00 00 50 6f 6e 74 69 61 63 20 46 69 72 65 62 69 72 64 09 00
      [401] 00 00 46 69 61 74 20 58 31 2d 39 0d 00 00 00 50 6f 72 73 63 68 65 20 39 31
      [426] 34 2d 32 0c 00 00 00 4c 6f 74 75 73 20 45 75 72 6f 70 61 0e 00 00 00 46 6f
      [451] 72 64 20 50 61 6e 74 65 72 61 20 4c 0c 00 00 00 46 65 72 72 61 72 69 20 44
      [476] 69 6e 6f 0d 00 00 00 4d 61 73 65 72 61 74 69 20 42 6f 72 61 0a 00 00 00 56
      [501] 6f 6c 76 6f 20 31 34 32 45
      
      $definition_levels_byte_length
      [1] NA
      
      $repetition_levels_byte_length
      [1] NA
      
      $num_nulls
      [1] NA
      
      $num_rows
      [1] NA
      
      $compressed_data
        [1] fd 03 34 09 00 00 00 4d 61 7a 64 61 20 52 58 34 0d 2e 0d 00 90 20 57 61 67
       [26] 0a 00 00 00 44 61 74 73 75 6e 20 37 31 30 0e 00 00 00 48 6f 72 6e 65 74 20
       [51] 34 20 44 72 69 76 65 11 19 12 50 53 70 6f 72 74 61 62 6f 75 74 07 00 00 00
       [76] 56 61 6c 69 61 6e 74 05 40 20 75 73 74 65 72 20 33 36 30 05 6c 20 65 72 63
      [101] 20 32 34 30 44 08 01 6c 05 0d 04 33 30 19 0c 00 38 1d 25 0c 38 30 43 0a 11
      [126] 25 10 34 35 30 53 45 32 0e 00 04 4c 0b 2e 1c 00 f0 43 4c 43 12 00 00 00 43
      [151] 61 64 69 6c 6c 61 63 20 46 6c 65 65 74 77 6f 6f 64 13 00 00 00 4c 69 6e 63
      [176] 6f 6c 6e 20 43 6f 6e 74 69 6e 65 6e 74 61 6c 11 00 00 00 43 68 72 79 73 6c
      [201] 65 72 20 49 6d 70 65 72 69 61 6c 01 86 1c 46 69 61 74 20 31 32 38 01 5d 28
      [226] 48 6f 6e 64 61 20 43 69 76 69 63 01 fa 34 54 6f 79 6f 74 61 20 43 6f 72 6f
      [251] 6c 6c 61 21 2b 1d 12 54 6e 61 10 00 00 00 44 6f 64 67 65 20 43 68 61 6c 6c
      [276] 65 6e 67 65 72 01 46 2c 41 4d 43 20 4a 61 76 65 6c 69 6e 0a 05 a3 1c 6d 61
      [301] 72 6f 20 5a 32 38 01 31 00 50 01 93 01 b0 18 69 72 65 62 69 72 64 21 22 05
      [326] 83 0c 58 31 2d 39 01 63 34 50 6f 72 73 63 68 65 20 39 31 34 2d 32 0c 01 cd
      [351] 28 6f 74 75 73 20 45 75 72 6f 70 61 01 96 34 46 6f 72 64 20 50 61 6e 74 65
      [376] 72 61 20 4c 01 22 2c 46 65 72 72 61 72 69 20 44 69 6e 6f 01 43 68 4d 61 73
      [401] 65 72 61 74 69 20 42 6f 72 61 0a 00 00 00 56 6f 6c 76 6f 20 31 34 32 45
      

---

    Code
      page2
    Output
      $page_type
      [1] "DATA_PAGE"
      
      $row_group
      [1] 0
      
      $column
      [1] 0
      
      $page_header_offset
      [1] 444
      
      $data_page_offset
      [1] 490
      
      $page_header_length
      [1] 46
      
      $compressed_page_size
      [1] 30
      
      $uncompressed_page_size
      [1] 28
      
      $codec
      [1] "SNAPPY"
      
      $num_values
      [1] 32
      
      $encoding
      [1] "RLE_DICTIONARY"
      
      $definition_level_encoding
      [1] "RLE"
      
      $repetition_level_encoding
      [1] "RLE"
      
      $has_repetition_levels
      [1] FALSE
      
      $has_definition_levels
      [1] TRUE
      
      $schema_column
      [1] 1
      
      $data_type
      [1] "BYTE_ARRAY"
      
      $repetition_type
      [1] "OPTIONAL"
      
      $page_header
       [1] 15 00 15 38 15 3c 2c 15 40 15 10 15 06 15 06 1c 36 00 28 0a 56 6f 6c 76 6f
      [26] 20 31 34 32 45 18 0b 41 4d 43 20 4a 61 76 65 6c 69 6e 00 00 00
      
      $data
       [1] 02 00 00 00 40 01 05 09 20 88 41 8a 39 28 a9 c5 9a 7b 30 ca 49 ab bd 38 eb
      [26] cd bb ff
      
      $definition_levels_byte_length
      [1] NA
      
      $repetition_levels_byte_length
      [1] NA
      
      $num_nulls
      [1] NA
      
      $num_rows
      [1] NA
      
      $compressed_data
       [1] 1c 6c 02 00 00 00 40 01 05 09 20 88 41 8a 39 28 a9 c5 9a 7b 30 ca 49 ab bd
      [26] 38 eb cd bb ff
      

# read_parquet_page for trick v2 data page

    Code
      read_parquet_page(pf, 4L)
    Output
      $page_type
      [1] "DATA_PAGE_V2"
      
      $row_group
      [1] 0
      
      $column
      [1] 0
      
      $page_header_offset
      [1] 4
      
      $data_page_offset
      [1] 27
      
      $page_header_length
      [1] 23
      
      $compressed_page_size
      [1] 46
      
      $uncompressed_page_size
      [1] 26
      
      $codec
      [1] "GZIP"
      
      $num_values
      [1] 68
      
      $encoding
      [1] "RLE"
      
      $definition_level_encoding
      [1] NA
      
      $repetition_level_encoding
      [1] NA
      
      $has_repetition_levels
      [1] FALSE
      
      $has_definition_levels
      [1] TRUE
      
      $schema_column
      [1] 1
      
      $data_type
      [1] "BOOLEAN"
      
      $repetition_type
      [1] "OPTIONAL"
      
      $page_header
       [1] 15 06 15 34 15 5c 5c 15 88 01 15 0c 15 88 01 15 06 15 16 15 04 00 00
      
      $data
       [1] 88 01 07 fb 7f 7f 1c 01 09 fe fb 09 00 00 00 11 cd d9 6c 0e 9b 33 b7 39
      
      $definition_levels_byte_length
      [1] 11
      
      $repetition_levels_byte_length
      [1] 2
      
      $num_nulls
      [1] 6
      
      $num_rows
      [1] 68
      
      $compressed_data
       [1] 88 01 07 fb 7f 7f 1c 01 09 fe fb bf 3f 1f 8b 08 00 00 00 00 00 00 00 e3 64
      [26] 60 60 10 3c 7b 33 87 6f b6 f1 76 4b 00 73 f2 49 94 0d 00 00 00
      

# MemStream

    Code
      .Call(test_memstream)
    Output
       [1] 54 68 69 73 20 69 73 20 61 20 74 65 73 74 0a 54 68 69 73 20 69 73 20 61 20
      [26] 74 65 73 74 0a 54 68 69 73 20 69 73 20 61 20 74 65 73 74 0a 54 68 69 73 20
      [51] 69 73 20 61 20 74 65 73 74 0a 54 68 69 73 20 69 73 20 61 20 74 65 73 74 0a
    Code
      cat(rawToChar(.Call(test_memstream)))
    Output
      This is a test
      This is a test
      This is a test
      This is a test
      This is a test

# DELTA_BIT_PACKED decoding

    Code
      stat
    Output
      $status
      [1] 0
      
      $stdout
      [1] ""
      
      $stderr
      NULL
      
      $timeout
      [1] FALSE
      

---

    Code
      stat
    Output
      $status
      [1] 0
      
      $stdout
      [1] ""
      
      $stderr
      NULL
      
      $timeout
      [1] FALSE
      

---

    Code
      stat
    Output
      $status
      [1] 0
      
      $stdout
      [1] ""
      
      $stderr
      NULL
      
      $timeout
      [1] FALSE
      

---

    Code
      stat
    Output
      $status
      [1] 0
      
      $stdout
      [1] ""
      
      $stderr
      NULL
      
      $timeout
      [1] FALSE
      

---

    Code
      stat
    Output
      $status
      [1] 0
      
      $stdout
      [1] ""
      
      $stderr
      NULL
      
      $timeout
      [1] FALSE
      

# DELTA_BINARY_PACKED edge cases

    Code
      p1 <- read_parquet_page(pf, pgs$page_header_offset[1])
      dbp1 <- p1$data[(p1$definition_levels_byte_length + p1$
        repetition_levels_byte_length + 1):length(p1$data)]
      dbp_decode_int(dbp1)
    Output
      [1] 0 1
    Code
      p3 <- read_parquet_page(pf, pgs$page_header_offset[3])
      dbp3 <- p3$data
      dbp_decode_int(dbp3)
    Output
      [1] -128  127
    Code
      p4 <- read_parquet_page(pf, pgs$page_header_offset[4])
      dbp4 <- p4$data
      dbp_decode_int(dbp4)
    Output
      [1] -32768  32767
    Code
      p5 <- read_parquet_page(pf, pgs$page_header_offset[5])
      dbp5 <- p5$data
      dbp_decode_int(dbp5)
    Output
      [1]         NA 2147483647

# DELTA_BIANRY_PACKED INT64

    Code
      dbp_decode_int(dt)
    Output
      [1] -100   -1    0    2    4    5  100

