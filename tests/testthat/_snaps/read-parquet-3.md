# TIMESTAMP

    Code
      as.data.frame(read_parquet(tmp))
    Output
                         ts
      1 2024-08-20 20:37:35
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name  r_type  type type_length repetition_type   converted_type
      1 schema    <NA>  <NA>          NA            <NA>             <NA>
      2     ts POSIXct INT64          NA        REQUIRED TIMESTAMP_MILLIS
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2 TIMESTAM....           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "TIMESTAMP"
      
      $is_adjusted_to_utc
      [1] TRUE
      
      $unit
      [1] "MILLIS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
                         ts
      1 2024-08-20 20:37:35
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name  r_type  type type_length repetition_type   converted_type
      1 schema    <NA>  <NA>          NA            <NA>             <NA>
      2     ts POSIXct INT64          NA        REQUIRED TIMESTAMP_MICROS
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2 TIMESTAM....           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "TIMESTAMP"
      
      $is_adjusted_to_utc
      [1] TRUE
      
      $unit
      [1] "MICROS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
                         ts
      1 2024-08-20 20:37:35
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2     ts POSIXct INT64          NA        REQUIRED           <NA> TIMESTAM....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "TIMESTAMP"
      
      $is_adjusted_to_utc
      [1] TRUE
      
      $unit
      [1] "NANOS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      

# TIMESTAMP, dictionary

    Code
      as.data.frame(read_parquet(tmp))
    Output
                         ts
      1 2024-08-20 20:37:35
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name  r_type  type type_length repetition_type   converted_type
      1 schema    <NA>  <NA>          NA            <NA>             <NA>
      2     ts POSIXct INT64          NA        REQUIRED TIMESTAMP_MILLIS
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2 TIMESTAM....           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "TIMESTAMP"
      
      $is_adjusted_to_utc
      [1] TRUE
      
      $unit
      [1] "MILLIS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
                         ts
      1 2024-08-20 20:37:35
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name  r_type  type type_length repetition_type   converted_type
      1 schema    <NA>  <NA>          NA            <NA>             <NA>
      2     ts POSIXct INT64          NA        REQUIRED TIMESTAMP_MICROS
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2 TIMESTAM....           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "TIMESTAMP"
      
      $is_adjusted_to_utc
      [1] TRUE
      
      $unit
      [1] "MICROS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
                         ts
      1 2024-08-20 20:37:35
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2     ts POSIXct INT64          NA        REQUIRED           <NA> TIMESTAM....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "TIMESTAMP"
      
      $is_adjusted_to_utc
      [1] TRUE
      
      $unit
      [1] "NANOS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      

# TIMESTAMP_MILLIS, TIMESTAMP_MICROS

    Code
      as.data.frame(read_parquet(tmp))
    Output
                         ts
      1 2024-08-20 20:37:35
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type   converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>             <NA>             
      2     ts double INT64          NA        REQUIRED TIMESTAMP_MILLIS             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
                         ts
      1 2024-08-20 20:37:35
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name  r_type  type type_length repetition_type   converted_type
      1 schema    <NA>  <NA>          NA            <NA>             <NA>
      2     ts POSIXct INT64          NA        REQUIRED TIMESTAMP_MICROS
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2                        NA    NA        NA       NA

# TIMESTAMP_MILLIS, TIMESTAMP_MICROS, dictionary

    Code
      as.data.frame(read_parquet(tmp))
    Output
                         ts
      1 2024-08-20 20:37:35
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type   converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>             <NA>             
      2     ts double INT64          NA        REQUIRED TIMESTAMP_MILLIS             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
                         ts
      1 2024-08-20 20:37:35
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name  r_type  type type_length repetition_type   converted_type
      1 schema    <NA>  <NA>          NA            <NA>             <NA>
      2     ts POSIXct INT64          NA        REQUIRED TIMESTAMP_MICROS
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2                        NA    NA        NA       NA
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      

# TIME

    Code
      as.data.frame(read_parquet(tmp))
    Output
              dt
      1 03:02:01
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2     dt    hms INT32          NA        REQUIRED    TIME_MILLIS TIME, TR....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "TIME"
      
      $is_adjusted_to_utc
      [1] TRUE
      
      $unit
      [1] "MILLIS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
              dt
      1 03:02:01
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2     dt    hms INT64          NA        REQUIRED    TIME_MICROS TIME, TR....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
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
      

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
              dt
      1 03:02:01
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2     dt    hms INT64          NA        REQUIRED           <NA> TIME, TR....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "TIME"
      
      $is_adjusted_to_utc
      [1] TRUE
      
      $unit
      [1] "NANOS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      

# TIME_MILLIS, TIME_MICROS

    Code
      as.data.frame(read_parquet(tmp))
    Output
              dt
      1 03:02:01
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2     dt    hms INT32          NA        REQUIRED    TIME_MILLIS             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
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
              dt
      1 03:02:01
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2     dt    hms INT64          NA        REQUIRED    TIME_MICROS             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      NULL
      

# TIME, dictionary

    Code
      as.data.frame(read_parquet(tmp))
    Output
              dt
      1 03:02:01
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2     dt    hms INT32          NA        REQUIRED    TIME_MILLIS TIME, TR....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "TIME"
      
      $is_adjusted_to_utc
      [1] TRUE
      
      $unit
      [1] "MILLIS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
              dt
      1 03:02:01
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2     dt    hms INT64          NA        REQUIRED    TIME_MICROS TIME, TR....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
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
      
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
              dt
      1 03:02:01
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2     dt    hms INT64          NA        REQUIRED           <NA> TIME, TR....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      $type
      [1] "TIME"
      
      $is_adjusted_to_utc
      [1] TRUE
      
      $unit
      [1] "NANOS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      

# TIME_MILLIS, TIME_MICROS, dictionary

    Code
      as.data.frame(read_parquet(tmp))
    Output
              dt
      1 03:02:01
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2     dt    hms INT32          NA        REQUIRED    TIME_MILLIS             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      NULL
      
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
              dt
      1 03:02:01
    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2     dt    hms INT64          NA        REQUIRED    TIME_MICROS             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    Output
      [[1]]
      NULL
      
      [[2]]
      NULL
      
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      

