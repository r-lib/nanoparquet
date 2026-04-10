# LIST schema

    Code
      as.data.frame(read_parquet_schema(pf))
    Output
                file_name r_col    name       r_type   type type_length
      1 data/list.parquet    NA  schema         <NA>   <NA>          NA
      2 data/list.parquet     1       a      integer  INT32          NA
      3 data/list.parquet     2       l list(double)   <NA>          NA
      4 data/list.parquet     2    list         <NA>   <NA>          NA
      5 data/list.parquet     2 element         <NA> DOUBLE          NA
        repetition_type converted_type logical_type num_children scale precision
      1        REQUIRED           <NA>                         2    NA        NA
      2        OPTIONAL           <NA>                        NA    NA        NA
      3        OPTIONAL           LIST         LIST            1    NA        NA
      4        REPEATED           <NA>                         1    NA        NA
      5        OPTIONAL           <NA>                        NA    NA        NA
        field_id children
      1       NA         
      2       NA         
      3       NA        4
      4       NA        5
      5       NA         

