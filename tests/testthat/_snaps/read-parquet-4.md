# DECIMAL converted type

    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1 100
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        REQUIRED        DECIMAL             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA     2         5       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      NULL
      

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1 100
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        REQUIRED        DECIMAL             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA     2         5       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      NULL
      

# DECIMAL in BA dict

    Code
      as.data.frame(read_parquet(pf))
    Output
          l1   l2   l3   l4
      1  0.1  0.1  0.1  0.1
      2 -0.1 -0.1 -0.1 -0.1

