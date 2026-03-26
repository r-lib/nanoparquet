# REPEATED columns, no LIST

    Code
      as.data.frame(read_parquet(pf, col_select = 1:2))
    Output
        Int32_list             String_list
      1 0, 1, 2, 3     foo, zero, one, two
      2                              three
      3          4                    four
      4 5, 6, 7, 8 five, six, seven, eight

