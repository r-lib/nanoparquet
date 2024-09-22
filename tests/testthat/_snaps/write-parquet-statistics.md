# null_count is written

    Code
      as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]][, c("row_group",
        "column", "null_count")])
    Output
         row_group column null_count
      1          0      0          1
      2          0      1          1
      3          0      2          1
      4          0      3          1
      5          0      4          1
      6          0      5          1
      7          0      6          1
      8          0      7          1
      9          0      8          1
      10         0      9          1
      11         0     10          0
      12         0     11          0
      13         0     12          0
      14         1      0          0
      15         1      1          0
      16         1      2          0
      17         1      3          0
      18         1      4          0
      19         1      5          0
      20         1      6          0
      21         1      7          0
      22         1      8          0
      23         1      9          0
      24         1     10          1
      25         1     11          1
      26         1     12          1
      27         2      0          0
      28         2      1          0
      29         2      2          0
      30         2      3          0
      31         2      4          0
      32         2      5          0
      33         2      6          0
      34         2      7          0
      35         2      8          0
      36         2      9          0
      37         2     10          0
      38         2     11          0
      39         2     12          0
      40         3      0          0
      41         3      1          0
      42         3      2          0
      43         3      3          0
      44         3      4          0
      45         3      5          0
      46         3      6          0
      47         3      7          0
      48         3      8          0
      49         3      9          0
      50         3     10          0
      51         3     11          0
      52         3     12          0

# min/max for integers

    Code
      do(compression = "snappy")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "snappy")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "uncompressed")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

# min/max for DATEs

    Code
      do()
    Output
      [[1]]
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2    day    Date INT32          NA        OPTIONAL           DATE         DATE
      3  count integer INT32          NA        REQUIRED         INT_32 INT, 32,....
        num_children scale precision field_id
      1            2    NA        NA       NA
      2           NA    NA        NA       NA
      3           NA    NA        NA       NA
      
      [[2]]
      [1] "2024-09-06" "2024-09-08" "2024-09-10" "2024-09-12" "2024-09-14"
      
      [[3]]
      [1] "2024-09-07" "2024-09-09" "2024-09-11" "2024-09-13" "2024-09-15"
      

# min/max for double -> signed integers

    Code
      do(compression = "snappy")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "snappy")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "uncompressed")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

# min/max for double -> unsigned integers

    Code
      do(compression = "snappy")
    Output
      [[1]]
      [1]  1  1  0 NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed")
    Output
      [[1]]
      [1]  1  1  0 NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "snappy")
    Output
      [[1]]
      [1]  1  1  0 NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "uncompressed")
    Output
      [[1]]
      [1]  1  1  0 NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

# minmax for double -> INT32 TIME(MULLIS)

    Code
      do(compression = "snappy")
    Output
      [[1]]
      [1]     1000  -100000 -1000000       NA
      
      [[2]]
      [1]    5000  100000 1000000      NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed")
    Output
      [[1]]
      [1]     1000  -100000 -1000000       NA
      
      [[2]]
      [1]    5000  100000 1000000      NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "snappy")
    Output
      [[1]]
      [1]     1000  -100000 -1000000       NA
      
      [[2]]
      [1]    5000  100000 1000000      NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "uncompressed")
    Output
      [[1]]
      [1]     1000  -100000 -1000000       NA
      
      [[2]]
      [1]    5000  100000 1000000      NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

# min/max for DOUBLE

    Code
      do(compression = "snappy")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "snappy")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "uncompressed")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

# min/max for FLOAT

    Code
      do(compression = "snappy")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "snappy")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "uncompressed")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

# min/max for integer -> INT64

    Code
      do(compression = "snappy")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "snappy")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "uncompressed")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

# min/max for REALSXP -> INT64

    Code
      do(compression = "snappy")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "snappy")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "uncompressed")
    Output
      [[1]]
      [1]     1  -100 -1000    NA
      
      [[2]]
      [1]    5  100 1000   NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

# min/max for STRING

    Code
      do(compression = "snappy")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "snappy")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "uncompressed")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "snappy", type = "JSON")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed", type = "JSON")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "snappy", type = "JSON")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "uncompressed", type = "JSON")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "snappy", type = "BSON")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed", type = "BSON")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "snappy", type = "BSON")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "uncompressed", type = "BSON")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "snappy", type = "ENUM")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed", type = "ENUM")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "snappy", type = "ENUM")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(encoding = "RLE_DICTIONARY", compression = "uncompressed", type = "ENUM")
    Output
      [[1]]
      [1] "a"   "!!!" "!"   NA   
      
      [[2]]
      [1] "e"   "~~~" "~"   NA   
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

# min/max for REALSXP -> TIMESTAMP (INT64)

    Code
      do(compression = "snappy")
    Output
      [[1]]
      [1]  1e+06 -1e+08 -1e+09     NA
      
      [[2]]
      [1] 5e+06 1e+08 1e+09    NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

---

    Code
      do(compression = "uncompressed")
    Output
      [[1]]
      [1]  1e+06 -1e+08 -1e+09     NA
      
      [[2]]
      [1] 5e+06 1e+08 1e+09    NA
      
      [[3]]
      [1] TRUE TRUE TRUE   NA
      
      [[4]]
      [1] TRUE TRUE TRUE   NA
      

