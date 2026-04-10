# REPEATED columns, no LIST

    Code
      as.data.frame(read_parquet(pf))
    Output
        Int32_list             String_list
      1 0, 1, 2, 3     foo, zero, one, two
      2                              three
      3          4                    four
      4 5, 6, 7, 8 five, six, seven, eight

# Only 3-layer LIST is supported for now

    Code
      as.data.frame(read_parquet(pf))
    Condition
      Error in `read_parquet()`:
      ! Only three-layer LIST columns are supported, could not read Parquet file at 'data/old_list_structure.parquet' @ lib/ParquetReader.cpp:202

# LIST

    Code
      as.data.frame(read_parquet(test_path("data/list-req-req.parquet")))
    Output
              a
      1 1, 2, 3
      2        
      3       4
    Code
      as.data.frame(read_parquet(test_path("data/list-req-opt.parquet")))
    Output
               a
      1 1, NA, 3
      2         
      3        4
    Code
      as.data.frame(read_parquet(test_path("data/list-opt-req.parquet")))
    Output
              a
      1 1, 2, 3
      2        
      3    NULL
      4       4
    Code
      as.data.frame(read_parquet(test_path("data/list-opt-opt.parquet")))
    Output
               a
      1 1, NA, 3
      2         
      3     NULL
      4        4

---

    Code
      as.data.frame(read_parquet(test_path("data/list-v2-req-req.parquet")))
    Output
              a
      1 1, 2, 3
      2        
      3       4
    Code
      as.data.frame(read_parquet(test_path("data/list-v2-req-opt.parquet")))
    Output
               a
      1 1, NA, 3
      2         
      3        4
    Code
      as.data.frame(read_parquet(test_path("data/list-v2-opt-req.parquet")))
    Output
              a
      1 1, 2, 3
      2        
      3    NULL
      4       4
    Code
      as.data.frame(read_parquet(test_path("data/list-v2-opt-opt.parquet")))
    Output
               a
      1 1, NA, 3
      2         
      3     NULL
      4        4

---

    Code
      read_parquet_page(pf, pgoff)[elts]
    Output
      $has_repetition_levels
      [1] TRUE
      
      $has_definition_levels
      [1] TRUE
      
      $num_values
      [1] 10
      
      $num_rows
      [1] NA
      

---

    Code
      read_parquet_page(pf2, pgoff2)[elts]
    Output
      $has_repetition_levels
      [1] TRUE
      
      $has_definition_levels
      [1] TRUE
      
      $num_values
      [1] 5
      
      $num_rows
      [1] NA
      

---

    Code
      read_parquet_page(pf3, pgoff3)[elts]
    Output
      $has_repetition_levels
      [1] TRUE
      
      $has_definition_levels
      [1] TRUE
      
      $num_values
      [1] 5
      
      $num_rows
      [1] NA
      

---

    Code
      read_parquet_page(pf4, pgoff4)[elts]
    Output
      $has_repetition_levels
      [1] TRUE
      
      $has_definition_levels
      [1] TRUE
      
      $num_values
      [1] 6
      
      $num_rows
      [1] NA
      

---

    Code
      read_parquet_page(pf5, pgoff5)[elts]
    Output
      $has_repetition_levels
      [1] TRUE
      
      $has_definition_levels
      [1] TRUE
      
      $num_values
      [1] 6
      
      $num_rows
      [1] NA
      

