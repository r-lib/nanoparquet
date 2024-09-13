# errors

    Code
      parquet_options(num_rows_per_row_group = "foobar")
    Condition
      Error in `as_count()`:
      ! num_rows_per_row_group must be a count, i.e. an integer scalar

---

    Code
      write_parquet(df, tmp, row_groups = "foobar")
    Condition
      Error in `parse_row_groups()`:
      ! Row groups must be specified as a growing positive integer vector, starting with 1.
    Code
      write_parquet(df, tmp, row_groups = c(100L, 1L))
    Condition
      Error in `parse_row_groups()`:
      ! Row groups must be specified as a growing positive integer vector, starting with 1.
    Code
      write_parquet(df, tmp, row_groups = c(1L, 100L))
    Condition
      Error in `write_parquet()`:
      ! Internal nanoparquet error, row index too large

# grouped df

    Code
      write_parquet(df, tmp)
    Message
      Ordering data frame according to row groups.

---

    Code
      as.data.frame(read_parquet(tmp)[, c("nam", "cyl")])
    Output
                         nam cyl
      1           Datsun 710   4
      2            Merc 240D   4
      3             Merc 230   4
      4             Fiat 128   4
      5          Honda Civic   4
      6       Toyota Corolla   4
      7        Toyota Corona   4
      8            Fiat X1-9   4
      9        Porsche 914-2   4
      10        Lotus Europa   4
      11          Volvo 142E   4
      12           Mazda RX4   6
      13       Mazda RX4 Wag   6
      14      Hornet 4 Drive   6
      15             Valiant   6
      16            Merc 280   6
      17           Merc 280C   6
      18        Ferrari Dino   6
      19   Hornet Sportabout   8
      20          Duster 360   8
      21          Merc 450SE   8
      22          Merc 450SL   8
      23         Merc 450SLC   8
      24  Cadillac Fleetwood   8
      25 Lincoln Continental   8
      26   Chrysler Imperial   8
      27    Dodge Challenger   8
      28         AMC Javelin   8
      29          Camaro Z28   8
      30    Pontiac Firebird   8
      31      Ford Pantera L   8
      32       Maserati Bora   8

