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
         uncompressed_page_size compressed_page_size crc num_values       encoding
      1                     509                  424  NA         32          PLAIN
      2                      28                   30  NA         32 RLE_DICTIONARY
      3                     200                  131  NA         25          PLAIN
      4                      28                   30  NA         32 RLE_DICTIONARY
      5                      24                   22  NA          3          PLAIN
      6                      16                   18  NA         32 RLE_DICTIONARY
      7                     216                  149  NA         27          PLAIN
      8                      28                   30  NA         32 RLE_DICTIONARY
      9                     176                  118  NA         22          PLAIN
      10                     28                   30  NA         32 RLE_DICTIONARY
      11                    176                  149  NA         22          PLAIN
      12                     28                   30  NA         32 RLE_DICTIONARY
      13                    232                  188  NA         29          PLAIN
      14                     28                   30  NA         32 RLE_DICTIONARY
      15                    240                  195  NA         30          PLAIN
      16                     28                   30  NA         32 RLE_DICTIONARY
      17                     16                   18  NA          2          PLAIN
      18                     12                   14  NA         32 RLE_DICTIONARY
      19                     16                   18  NA          2          PLAIN
      20                     14                   16  NA         32 RLE_DICTIONARY
      21                     24                   22  NA          3          PLAIN
      22                     16                   18  NA         32 RLE_DICTIONARY
      23                     48                   35  NA          6          PLAIN
      24                     20                   22  NA         32 RLE_DICTIONARY
      25                     25                   26  NA          5          PLAIN
      26                     20                   22  NA         32 RLE_DICTIONARY
         definition_level_encoding repetition_level_encoding page_header_offset
      1                       <NA>                      <NA>                  4
      2                        RLE                       RLE                444
      3                       <NA>                      <NA>                600
      4                        RLE                       RLE                747
      5                       <NA>                      <NA>                933
      6                        RLE                       RLE                969
      7                       <NA>                      <NA>               1143
      8                        RLE                       RLE               1308
      9                       <NA>                      <NA>               1495
      10                       RLE                       RLE               1629
      11                      <NA>                      <NA>               1814
      12                       RLE                       RLE               1979
      13                      <NA>                      <NA>               2166
      14                       RLE                       RLE               2370
      15                      <NA>                      <NA>               2555
      16                       RLE                       RLE               2766
      17                      <NA>                      <NA>               2953
      18                       RLE                       RLE               2985
      19                      <NA>                      <NA>               3154
      20                       RLE                       RLE               3186
      21                      <NA>                      <NA>               3357
      22                       RLE                       RLE               3393
      23                      <NA>                      <NA>               3568
      24                       RLE                       RLE               3617
      25                      <NA>                      <NA>               3796
      26                       RLE                       RLE               3836
         data_offset page_header_length
      1           20                 16
      2          490                 46
      3          616                 16
      4          808                 61
      5          947                 14
      6         1030                 61
      7         1159                 16
      8         1369                 61
      9         1511                 16
      10        1690                 61
      11        1830                 16
      12        2040                 61
      13        2182                 16
      14        2431                 61
      15        2571                 16
      16        2827                 61
      17        2967                 14
      18        3046                 61
      19        3168                 14
      20        3247                 61
      21        3371                 14
      22        3454                 61
      23        3582                 14
      24        3678                 61
      25        3810                 14
      26        3863                 27

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
      [1] 0
      
      $encoding
      [1] NA
      
      $definition_level_encoding
      [1] NA
      
      $repetition_level_encoding
      [1] NA
      
      $has_repetition_levels
      [1] NA
      
      $has_definition_levels
      [1] NA
      
      $schema_column
      [1] 1
      
      $data_type
      [1] "BYTE_ARRAY"
      
      $repetition_type
      [1] "OPTIONAL"
      
      $page_header
       [1] 15 04 15 fa 07 15 d0 06 4c 15 40 15 00 12 00 00
      
      $data
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
       [1] 1c 6c 02 00 00 00 40 01 05 09 20 88 41 8a 39 28 a9 c5 9a 7b 30 ca 49 ab bd
      [26] 38 eb cd bb ff
      

