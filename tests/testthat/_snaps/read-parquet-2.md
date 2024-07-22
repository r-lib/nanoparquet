# BOOLEAN

    Code
      as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    Output
      [1] "DATA_PAGE"
    Code
      as.data.frame(read_parquet(tmp))
    Output
            i
      1  TRUE
      2 FALSE
      3  TRUE
      4 FALSE
      5  TRUE
      6 FALSE

---

    Code
      as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    Output
      [1] "DATA_PAGE"
    Code
      as.data.frame(read_parquet(tmp))
    Output
            i
      1    NA
      2 FALSE
      3    NA
      4 FALSE
      5    NA
      6 FALSE

---

    Code
      read_parquet(tmp)[["i"]]
    Output
        [1]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [13]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [25]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [37]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [49]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [61]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [73]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [85]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [97]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [109]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [121]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [133]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [145]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [157]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [169]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [181]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [193]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE

---

    Code
      read_parquet(tmp)[["i"]]
    Output
        [1]    NA FALSE    NA FALSE    NA FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [13]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [25]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [37]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [49]  TRUE    NA  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [61]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [73]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [85]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
       [97]  TRUE FALSE    NA FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [109]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [121]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [133]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [145]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [157]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [169]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [181]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE
      [193]  TRUE FALSE  TRUE FALSE  TRUE FALSE  TRUE FALSE

# INT32

    Code
      as.data.frame(read_parquet(tmp))
    Output
          i
      1   1
      2   2
      3   3
      4   4
      5   5
      6   6
      7   7
      8   8
      9   9
      10 10

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
          i
      1   1
      2  NA
      3   3
      4   4
      5   5
      6  NA
      7   7
      8   8
      9   9
      10 NA

---

    Code
      as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    Output
      [1] "DICTIONARY_PAGE" "DATA_PAGE"      
    Code
      as.data.frame(read_parquet(tmp))
    Output
         i
      1  1
      2  2
      3  1
      4  2
      5  1
      6  2
      7  1
      8  2
      9  1
      10 2
      11 1
      12 2
      13 1
      14 2
      15 1
      16 2
      17 1
      18 2
      19 1
      20 2
      21 1
      22 2
      23 1
      24 2
      25 1
      26 2
      27 1
      28 2
      29 1
      30 2
      31 1
      32 2
      33 1
      34 2
      35 1
      36 2
      37 1
      38 2
      39 1
      40 2

---

    Code
      as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    Output
      [1] "DICTIONARY_PAGE" "DATA_PAGE"      
    Code
      as.data.frame(read_parquet(tmp))
    Output
          i
      1  NA
      2   2
      3   1
      4   2
      5  NA
      6   2
      7   1
      8   2
      9   1
      10 NA
      11  1
      12  2
      13  1
      14  2
      15  1
      16  2
      17  1
      18  2
      19  1
      20 NA
      21  1
      22  2
      23  1
      24  2
      25  1
      26  2
      27  1
      28  2
      29  1
      30  2
      31  1
      32  2
      33  1
      34  2
      35  1
      36  2
      37  1
      38  2
      39 NA
      40  2

# INT64

    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      i double INT64          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          i
      1   1
      2   2
      3   3
      4   4
      5   5
      6   6
      7   7
      8   8
      9   9
      10 10

---

    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      i double INT64          NA        OPTIONAL           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          i
      1   1
      2  NA
      3   3
      4   4
      5   5
      6  NA
      7   7
      8   8
      9   9
      10 NA

---

    Code
      as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    Output
      [1] "DICTIONARY_PAGE" "DATA_PAGE"      
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      i double INT64          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
         i
      1  1
      2  2
      3  1
      4  2
      5  1
      6  2
      7  1
      8  2
      9  1
      10 2
      11 1
      12 2
      13 1
      14 2
      15 1
      16 2
      17 1
      18 2
      19 1
      20 2
      21 1
      22 2
      23 1
      24 2
      25 1
      26 2
      27 1
      28 2
      29 1
      30 2
      31 1
      32 2
      33 1
      34 2
      35 1
      36 2
      37 1
      38 2
      39 1
      40 2

---

    Code
      as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    Output
      [1] "DICTIONARY_PAGE" "DATA_PAGE"      
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      i double INT64          NA        OPTIONAL           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          i
      1  NA
      2   2
      3   1
      4   2
      5  NA
      6   2
      7   1
      8   2
      9   1
      10 NA
      11  1
      12  2
      13  1
      14  2
      15  1
      16  2
      17  1
      18  2
      19  1
      20 NA
      21  1
      22  2
      23  1
      24  2
      25  1
      26  2
      27  1
      28  2
      29  1
      30  2
      31  1
      32  2
      33  1
      34  2
      35  1
      36  2
      37  1
      38  2
      39 NA
      40  2

# FLOAT

    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      i double FLOAT          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
           i
      1  0.5
      2  1.0
      3  1.5
      4  2.0
      5  2.5
      6  3.0
      7  3.5
      8  4.0
      9  4.5
      10 5.0

---

    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      i double FLOAT          NA        OPTIONAL           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
           i
      1  0.5
      2   NA
      3  1.5
      4  2.0
      5  2.5
      6   NA
      7  3.5
      8  4.0
      9  4.5
      10  NA

---

    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      i double FLOAT          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    Output
      [1] "DICTIONARY_PAGE" "DATA_PAGE"      
    Code
      as.data.frame(read_parquet(tmp))
    Output
           i
      1  0.5
      2  1.0
      3  0.5
      4  1.0
      5  0.5
      6  1.0
      7  0.5
      8  1.0
      9  0.5
      10 1.0
      11 0.5
      12 1.0
      13 0.5
      14 1.0
      15 0.5
      16 1.0
      17 0.5
      18 1.0
      19 0.5
      20 1.0
      21 0.5
      22 1.0
      23 0.5
      24 1.0
      25 0.5
      26 1.0
      27 0.5
      28 1.0
      29 0.5
      30 1.0
      31 0.5
      32 1.0
      33 0.5
      34 1.0
      35 0.5
      36 1.0
      37 0.5
      38 1.0
      39 0.5
      40 1.0

---

    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      i double FLOAT          NA        OPTIONAL           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    Output
      [1] "DICTIONARY_PAGE" "DATA_PAGE"      
    Code
      as.data.frame(read_parquet(tmp))
    Output
           i
      1   NA
      2  1.0
      3  0.5
      4  1.0
      5   NA
      6  1.0
      7  0.5
      8  1.0
      9  0.5
      10  NA
      11 0.5
      12 1.0
      13 0.5
      14 1.0
      15 0.5
      16 1.0
      17 0.5
      18 1.0
      19 0.5
      20  NA
      21 0.5
      22 1.0
      23 0.5
      24 1.0
      25 0.5
      26 1.0
      27 0.5
      28 1.0
      29 0.5
      30 1.0
      31 0.5
      32 1.0
      33 0.5
      34 1.0
      35 0.5
      36 1.0
      37 0.5
      38 1.0
      39  NA
      40 1.0

# DOUBLE

    Code
      as.data.frame(read_parquet(tmp))
    Output
           i
      1  0.5
      2  1.0
      3  1.5
      4  2.0
      5  2.5
      6  3.0
      7  3.5
      8  4.0
      9  4.5
      10 5.0

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
           i
      1  0.5
      2   NA
      3  1.5
      4  2.0
      5  2.5
      6   NA
      7  3.5
      8  4.0
      9  4.5
      10  NA

---

    Code
      as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    Output
      [1] "DICTIONARY_PAGE" "DATA_PAGE"      
    Code
      as.data.frame(read_parquet(tmp))
    Output
           i
      1  0.5
      2  1.0
      3  0.5
      4  1.0
      5  0.5
      6  1.0
      7  0.5
      8  1.0
      9  0.5
      10 1.0
      11 0.5
      12 1.0
      13 0.5
      14 1.0
      15 0.5
      16 1.0
      17 0.5
      18 1.0
      19 0.5
      20 1.0
      21 0.5
      22 1.0
      23 0.5
      24 1.0
      25 0.5
      26 1.0
      27 0.5
      28 1.0
      29 0.5
      30 1.0
      31 0.5
      32 1.0
      33 0.5
      34 1.0
      35 0.5
      36 1.0
      37 0.5
      38 1.0
      39 0.5
      40 1.0

---

    Code
      as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    Output
      [1] "DICTIONARY_PAGE" "DATA_PAGE"      
    Code
      as.data.frame(read_parquet(tmp))
    Output
           i
      1   NA
      2  1.0
      3  0.5
      4  1.0
      5   NA
      6  1.0
      7  0.5
      8  1.0
      9  0.5
      10  NA
      11 0.5
      12 1.0
      13 0.5
      14 1.0
      15 0.5
      16 1.0
      17 0.5
      18 1.0
      19 0.5
      20  NA
      21 0.5
      22 1.0
      23 0.5
      24 1.0
      25 0.5
      26 1.0
      27 0.5
      28 1.0
      29 0.5
      30 1.0
      31 0.5
      32 1.0
      33 0.5
      34 1.0
      35 0.5
      36 1.0
      37 0.5
      38 1.0
      39  NA
      40 1.0

