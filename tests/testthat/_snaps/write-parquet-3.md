# write_parquet -> INT32

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   2
      3   3
      4   4
      5   5
      6   6
      7   7
      8   8
      9   9
      10 10

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   2
      3   1
      4   2
      5   1
      6   2
      7   1
      8   2
      9   1
      10  2
      11  1
      12  2
      13  1
      14  2
      15  1
      16  2
      17  1
      18  2
      19  1
      20  2
      21  1
      22  2
      23  1
      24  2
      25  1
      26  2
      27  1
      28  2
      29  1
      30  2
      31  1
      32  2
      33  1
      34  2
      35  1
      36  2
      37  1
      38  2
      39  1
      40  2
      41  1
      42  2
      43  1
      44  2
      45  1
      46  2
      47  1
      48  2
      49  1
      50  2
      51  1
      52  2
      53  1
      54  2
      55  1
      56  2
      57  1
      58  2
      59  1
      60  2
      61  1
      62  2
      63  1
      64  2
      65  1
      66  2
      67  1
      68  2
      69  1
      70  2
      71  1
      72  2
      73  1
      74  2
      75  1
      76  2
      77  1
      78  2
      79  1
      80  2
      81  1
      82  2
      83  1
      84  2
      85  1
      86  2
      87  1
      88  2
      89  1
      90  2
      91  1
      92  2
      93  1
      94  2
      95  1
      96  2
      97  1
      98  2
      99  1
      100 2

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT32"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write a double vector as a Parquet INT32 type.
    Code
      write_parquet(d2, tmp, schema = parquet_schema("INT32"))
    Condition
      Error in `write_parquet()`:
      ! Cannot convert a double vector to Parquet type INT32.

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT32"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write a logical vector as a Parquet INT32 type.

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT32"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write a character vector as a Parquet INT32 type.

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT32"))
    Condition
      Error in `encode_arrow_schema_r()`:
      ! Unsuppoted types when writing Parquet file: list

# write_parquet -> INT64

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   2
      3   3
      4   4
      5   5
      6   6
      7   7
      8   8
      9   9
      10 10

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   2
      3   1
      4   2
      5   1
      6   2
      7   1
      8   2
      9   1
      10  2
      11  1
      12  2
      13  1
      14  2
      15  1
      16  2
      17  1
      18  2
      19  1
      20  2
      21  1
      22  2
      23  1
      24  2
      25  1
      26  2
      27  1
      28  2
      29  1
      30  2
      31  1
      32  2
      33  1
      34  2
      35  1
      36  2
      37  1
      38  2
      39  1
      40  2
      41  1
      42  2
      43  1
      44  2
      45  1
      46  2
      47  1
      48  2
      49  1
      50  2
      51  1
      52  2
      53  1
      54  2
      55  1
      56  2
      57  1
      58  2
      59  1
      60  2
      61  1
      62  2
      63  1
      64  2
      65  1
      66  2
      67  1
      68  2
      69  1
      70  2
      71  1
      72  2
      73  1
      74  2
      75  1
      76  2
      77  1
      78  2
      79  1
      80  2
      81  1
      82  2
      83  1
      84  2
      85  1
      86  2
      87  1
      88  2
      89  1
      90  2
      91  1
      92  2
      93  1
      94  2
      95  1
      96  2
      97  1
      98  2
      99  1
      100 2

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   2
      3   3
      4   4
      5   5
      6   6
      7   7
      8   8
      9   9
      10 10

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   2
      3   1
      4   2
      5   1
      6   2
      7   1
      8   2
      9   1
      10  2
      11  1
      12  2
      13  1
      14  2
      15  1
      16  2
      17  1
      18  2
      19  1
      20  2
      21  1
      22  2
      23  1
      24  2
      25  1
      26  2
      27  1
      28  2
      29  1
      30  2
      31  1
      32  2
      33  1
      34  2
      35  1
      36  2
      37  1
      38  2
      39  1
      40  2
      41  1
      42  2
      43  1
      44  2
      45  1
      46  2
      47  1
      48  2
      49  1
      50  2
      51  1
      52  2
      53  1
      54  2
      55  1
      56  2
      57  1
      58  2
      59  1
      60  2
      61  1
      62  2
      63  1
      64  2
      65  1
      66  2
      67  1
      68  2
      69  1
      70  2
      71  1
      72  2
      73  1
      74  2
      75  1
      76  2
      77  1
      78  2
      79  1
      80  2
      81  1
      82  2
      83  1
      84  2
      85  1
      86  2
      87  1
      88  2
      89  1
      90  2
      91  1
      92  2
      93  1
      94  2
      95  1
      96  2
      97  1
      98  2
      99  1
      100 2

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT64"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write a logical vector as a Parquet INT64 type.

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT64"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write a character vector as a Parquet INT64 type.

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT64"))
    Condition
      Error in `encode_arrow_schema_r()`:
      ! Unsuppoted types when writing Parquet file: list

# write_parquet -> INT96

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d POSIXct INT96          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      read_parquet_page(tmp, 4L)$data
    Output
        [1] fb ff ff ff ff ff ff ff ff ff ff ff fc ff ff ff ff ff ff ff ff ff ff ff fd
       [26] ff ff ff ff ff ff ff ff ff ff ff fe ff ff ff ff ff ff ff ff ff ff ff ff ff
       [51] ff ff ff ff ff ff ff ff ff ff 00 00 00 00 00 00 00 00 00 00 00 00 01 00 00
       [76] 00 00 00 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00 00 00 00 03 00 00 00
      [101] 00 00 00 00 00 00 00 00 04 00 00 00 00 00 00 00 00 00 00 00 05 00 00 00 00
      [126] 00 00 00 00 00 00 00

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d POSIXct INT96          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      read_parquet_page(tmp, 4L)$data
    Output
       [1] ff ff ff ff ff ff ff ff ff ff ff ff 00 00 00 00 00 00 00 00 00 00 00 00 01
      [26] 00 00 00 00 00 00 00 00 00 00 00

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d POSIXct INT96          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      read_parquet_page(tmp, 4L)$data
    Output
        [1] fb ff ff ff ff ff ff ff ff ff ff ff fc ff ff ff ff ff ff ff ff ff ff ff fd
       [26] ff ff ff ff ff ff ff ff ff ff ff fe ff ff ff ff ff ff ff ff ff ff ff ff ff
       [51] ff ff ff ff ff ff ff ff ff ff 00 00 00 00 00 00 00 00 00 00 00 00 01 00 00
       [76] 00 00 00 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00 00 00 00 03 00 00 00
      [101] 00 00 00 00 00 00 00 00 04 00 00 00 00 00 00 00 00 00 00 00 05 00 00 00 00
      [126] 00 00 00 00 00 00 00

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d POSIXct INT96          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      read_parquet_page(tmp, 4L)$data
    Output
       [1] ff ff ff ff ff ff ff ff ff ff ff ff 00 00 00 00 00 00 00 00 00 00 00 00 01
      [26] 00 00 00 00 00 00 00 00 00 00 00

# write_parquet -> FLOAT

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double FLOAT          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1  -5
      2  -4
      3  -3
      4  -2
      5  -1
      6   0
      7   1
      8   2
      9   3
      10  4
      11  5

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double FLOAT          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1  -1
      2   0
      3   1
      4  -1
      5   0
      6   1
      7  -1
      8   0
      9   1
      10 -1
      11  0
      12  1
      13 -1
      14  0
      15  1
      16 -1
      17  0
      18  1
      19 -1
      20  0
      21  1
      22 -1
      23  0
      24  1
      25 -1
      26  0
      27  1
      28 -1
      29  0
      30  1
      31 -1
      32  0
      33  1
      34 -1
      35  0
      36  1
      37 -1
      38  0
      39  1
      40 -1
      41  0
      42  1
      43 -1
      44  0
      45  1
      46 -1
      47  0
      48  1
      49 -1
      50  0
      51  1
      52 -1
      53  0
      54  1
      55 -1
      56  0
      57  1
      58 -1
      59  0
      60  1
      61 -1
      62  0
      63  1
      64 -1
      65  0
      66  1
      67 -1
      68  0
      69  1
      70 -1
      71  0
      72  1
      73 -1
      74  0
      75  1
      76 -1
      77  0
      78  1
      79 -1
      80  0
      81  1
      82 -1
      83  0
      84  1
      85 -1
      86  0
      87  1
      88 -1
      89  0
      90  1

# write_parquet -> FIXED_LEN_BYTE_ARRAY

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type                 type type_length repetition_type converted_type
      1 schema   <NA>                 <NA>          NA            <NA>           <NA>
      2      d    raw FIXED_LEN_BYTE_ARRAY           3        REQUIRED           <NA>
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2                        NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
                 d
      1 66, 6f, 6f
      2 62, 61, 72
      3 61, 61, 61

# write_parquet -> STRING

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name    r_type       type type_length repetition_type converted_type
      1 schema      <NA>       <NA>          NA            <NA>           <NA>
      2      d character BYTE_ARRAY          NA        REQUIRED           UTF8
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2       STRING           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
             d
      1    foo
      2    bar
      3 foobar

# write_parquet -> ENUM

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name    r_type       type type_length repetition_type converted_type
      1 schema      <NA>       <NA>          NA            <NA>           <NA>
      2      d character BYTE_ARRAY          NA        REQUIRED           ENUM
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2         ENUM           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
             d
      1    foo
      2    bar
      3 foobar

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name    r_type       type type_length repetition_type converted_type
      1 schema    factor       <NA>          NA            <NA>           <NA>
      2      d character BYTE_ARRAY          NA        REQUIRED           ENUM
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2         ENUM           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
             d
      1    foo
      2    bar
      3 foobar

