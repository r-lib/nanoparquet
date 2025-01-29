# read a subset, edge cases

    Code
      read_parquet(tmp, col_select = integer())
    Output
      # A data frame: 32 x 0

# error if a column is requested multiple times

    Code
      read_parquet(tmp, col_select = c(1, 1))
    Condition
      Error in `read_parquet()`:
      ! Column 1 selected multiple times in `read_parquet()`.
    Code
      read_parquet(tmp, col_select = c(3, 4, 5, 3))
    Condition
      Error in `read_parquet()`:
      ! Column 3 selected multiple times in `read_parquet()`.
    Code
      read_parquet(tmp, col_select = c(3:4, 4:3))
    Condition
      Error in `read_parquet()`:
      ! Columns 3, 4 selected multiple times in `read_parquet()`.

