# read a subset, edge cases

    Code
      read_parquet(tmp, col_select = integer())
    Output
      # A data frame: 32 x 0
    Code
      read_parquet(tmp, col_select = character())
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
    Code
      read_parquet(tmp, col_select = "foo")
    Condition
      Error in `read_parquet()`:
      ! Column foo does not exist in Parquet file
    Code
      read_parquet(tmp, col_select = c("foo", "bar"))
    Condition
      Error in `read_parquet()`:
      ! Columns foo, bar do not exist in Parquet file
    Code
      read_parquet(tmp, col_select = c("nam", "nam"))
    Condition
      Error in `read_parquet()`:
      ! Column nam selected multiple times in `read_parquet()`.
    Code
      read_parquet(tmp, col_select = c("cyl", "disp", "hp", "cyl"))
    Condition
      Error in `read_parquet()`:
      ! Column cyl selected multiple times in `read_parquet()`.
    Code
      read_parquet(tmp, col_select = c("cyl", "disp", "disp", "cyl"))
    Condition
      Error in `read_parquet()`:
      ! Columns cyl, disp selected multiple times in `read_parquet()`.

