# obj_sum.nanoparquet_logical_type

    Code
      lapply(sch$logical_type, obj_sum.nanoparquet_logical_type)
    Output
      [[1]]
      [1] "INT(8, TRUE)"
      
      [[2]]
      [1] "DECIMAL(3, 1)"
      
      [[3]]
      [1] "TIME(TRUE, MILLIS)"
      
      [[4]]
      [1] "TIMESTAMP(TRUE, MICROS)"
      

---

    Code
      obj_sum.nanoparquet_logical_type(read_parquet_schema(tmp)$logical_type[[2]])
    Output
      [1] "TIMESTAMP(TRUE, MICROS)"

