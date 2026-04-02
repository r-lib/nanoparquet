# LIST schema

    Code
      as.data.frame(read_parquet_schema(pf))
    Output
                file_name    name       r_type   type type_length repetition_type
      1 data/list.parquet  schema         <NA>   <NA>          NA        REQUIRED
      2 data/list.parquet       a      integer  INT32          NA        OPTIONAL
      3 data/list.parquet       l list(double)   <NA>          NA        OPTIONAL
      4 data/list.parquet    list         <NA>   <NA>          NA        REPEATED
      5 data/list.parquet element         <NA> DOUBLE          NA        OPTIONAL
        converted_type logical_type num_children scale precision field_id children
      1           <NA>                         2    NA        NA       NA         
      2           <NA>                        NA    NA        NA       NA         
      3           LIST         LIST            1    NA        NA       NA        4
      4           <NA>                         1    NA        NA       NA        5
      5           <NA>                        NA    NA        NA       NA         

