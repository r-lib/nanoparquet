# appending all-NA rows to REQUIRED columns gives a clear error

    Code
      append_parquet(df_nas, tmp)
    Condition
      Error in `check_schema_required_cols()`:
      ! Parquet schema does not allow missing values for columns:id, value, label

