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
      ! Only three-layer LIST columns are supported, could not read Parquet file at 'data/old_list_structure.parquet' @ lib/ParquetReader.cpp:199

