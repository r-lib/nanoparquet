# write_parquet -> INT32

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL           <NA>             
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
      11 NA

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
           d
      1   NA
      2    1
      3    2
      4    1
      5    2
      6    1
      7    2
      8    1
      9    2
      10   1
      11   2
      12   1
      13   2
      14   1
      15   2
      16   1
      17   2
      18   1
      19   2
      20   1
      21   2
      22   1
      23   2
      24   1
      25   2
      26   1
      27   2
      28   1
      29   2
      30   1
      31   2
      32   1
      33   2
      34   1
      35   2
      36   1
      37   2
      38   1
      39   2
      40   1
      41   2
      42   1
      43   2
      44   1
      45   2
      46   1
      47   2
      48   1
      49   2
      50   1
      51   2
      52   1
      53   2
      54   1
      55   2
      56   1
      57   2
      58   1
      59   2
      60   1
      61   2
      62   1
      63   2
      64   1
      65   2
      66   1
      67   2
      68   1
      69   2
      70   1
      71   2
      72   1
      73   2
      74   1
      75   2
      76   1
      77   2
      78   1
      79   2
      80   1
      81   2
      82   1
      83   2
      84   1
      85   2
      86   1
      87   2
      88   1
      89   2
      90   1
      91   2
      92   1
      93   2
      94   1
      95   2
      96   1
      97   2
      98   1
      99   2
      100  1
      101  2

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
      Error in `write_parquet()`:
      ! Cannot write a list as a Parquet INT32 type.

# write_parquet -> INT64

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        OPTIONAL           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1  NA
      2   1
      3   2
      4   3
      5   4
      6   5
      7   6
      8   7
      9   8
      10  9
      11 10

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        OPTIONAL           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
           d
      1   NA
      2    1
      3    2
      4    1
      5    2
      6    1
      7    2
      8    1
      9    2
      10   1
      11   2
      12   1
      13   2
      14   1
      15   2
      16   1
      17   2
      18   1
      19   2
      20   1
      21   2
      22   1
      23   2
      24   1
      25   2
      26   1
      27   2
      28   1
      29   2
      30   1
      31   2
      32   1
      33   2
      34   1
      35   2
      36   1
      37   2
      38   1
      39   2
      40   1
      41   2
      42   1
      43   2
      44   1
      45   2
      46   1
      47   2
      48   1
      49   2
      50   1
      51   2
      52   1
      53   2
      54   1
      55   2
      56   1
      57   2
      58   1
      59   2
      60   1
      61   2
      62   1
      63   2
      64   1
      65   2
      66   1
      67   2
      68   1
      69   2
      70   1
      71   2
      72   1
      73   2
      74   1
      75   2
      76   1
      77   2
      78   1
      79   2
      80   1
      81   2
      82   1
      83   2
      84   1
      85   2
      86   1
      87   2
      88   1
      89   2
      90   1
      91   2
      92   1
      93   2
      94   1
      95   2
      96   1
      97   2
      98   1
      99   2
      100  1
      101  2

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        OPTIONAL           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1  NA
      2   1
      3   2
      4   3
      5   4
      6   5
      7   6
      8   7
      9   8
      10  9
      11 10

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        OPTIONAL           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
           d
      1   NA
      2    1
      3    2
      4    1
      5    2
      6    1
      7    2
      8    1
      9    2
      10   1
      11   2
      12   1
      13   2
      14   1
      15   2
      16   1
      17   2
      18   1
      19   2
      20   1
      21   2
      22   1
      23   2
      24   1
      25   2
      26   1
      27   2
      28   1
      29   2
      30   1
      31   2
      32   1
      33   2
      34   1
      35   2
      36   1
      37   2
      38   1
      39   2
      40   1
      41   2
      42   1
      43   2
      44   1
      45   2
      46   1
      47   2
      48   1
      49   2
      50   1
      51   2
      52   1
      53   2
      54   1
      55   2
      56   1
      57   2
      58   1
      59   2
      60   1
      61   2
      62   1
      63   2
      64   1
      65   2
      66   1
      67   2
      68   1
      69   2
      70   1
      71   2
      72   1
      73   2
      74   1
      75   2
      76   1
      77   2
      78   1
      79   2
      80   1
      81   2
      82   1
      83   2
      84   1
      85   2
      86   1
      87   2
      88   1
      89   2
      90   1
      91   2
      92   1
      93   2
      94   1
      95   2
      96   1
      97   2
      98   1
      99   2
      100  1
      101  2

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
      Error in `write_parquet()`:
      ! Cannot write a list as a Parquet INT64 type.

# write_parquet -> INT96

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d POSIXct INT96          NA        OPTIONAL           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      read_parquet_page(tmp, 4L)$data
    Output
        [1] 04 00 00 00 16 01 03 00 fb ff ff ff ff ff ff ff ff ff ff ff fc ff ff ff ff
       [26] ff ff ff ff ff ff ff fd ff ff ff ff ff ff ff ff ff ff ff fe ff ff ff ff ff
       [51] ff ff ff ff ff ff ff ff ff ff ff ff ff ff ff ff ff ff 00 00 00 00 00 00 00
       [76] 00 00 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00 02 00 00 00 00 00 00 00
      [101] 00 00 00 00 03 00 00 00 00 00 00 00 00 00 00 00 04 00 00 00 00 00 00 00 00
      [126] 00 00 00 05 00 00 00 00 00 00 00 00 00 00 00

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d POSIXct INT96          NA        OPTIONAL           <NA>             
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
      2      d POSIXct INT96          NA        OPTIONAL           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      read_parquet_page(tmp, 4L)$data
    Output
        [1] 04 00 00 00 16 01 03 00 fb ff ff ff ff ff ff ff ff ff ff ff fc ff ff ff ff
       [26] ff ff ff ff ff ff ff fd ff ff ff ff ff ff ff ff ff ff ff fe ff ff ff ff ff
       [51] ff ff ff ff ff ff ff ff ff ff ff ff ff ff ff ff ff ff 00 00 00 00 00 00 00
       [76] 00 00 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00 02 00 00 00 00 00 00 00
      [101] 00 00 00 00 03 00 00 00 00 00 00 00 00 00 00 00 04 00 00 00 00 00 00 00 00
      [126] 00 00 00 05 00 00 00 00 00 00 00 00 00 00 00

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d POSIXct INT96          NA        OPTIONAL           <NA>             
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
      2      d double FLOAT          NA        OPTIONAL           <NA>             
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
      12 NA

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double FLOAT          NA        OPTIONAL           <NA>             
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
      91 NA

# write_parquet -> BYTE_ARRAY

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type       type type_length repetition_type converted_type
      1 schema   <NA>       <NA>          NA            <NA>           <NA>
      2      d    raw BYTE_ARRAY          NA        OPTIONAL           <NA>
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2                        NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
                             d
      1             66, 6f, 6f
      2             62, 61, 72
      3 66, 6f, 6f, 62, 61, 72
      4                   NULL

# write_parquet -> FIXED_LEN_BYTE_ARRAY

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type                 type type_length repetition_type converted_type
      1 schema   <NA>                 <NA>          NA            <NA>           <NA>
      2      d    raw FIXED_LEN_BYTE_ARRAY           3        OPTIONAL           <NA>
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
      4       NULL

# write_parquet -> STRING

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name    r_type       type type_length repetition_type converted_type
      1 schema      <NA>       <NA>          NA            <NA>           <NA>
      2      d character BYTE_ARRAY          NA        OPTIONAL           UTF8
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
      4   <NA>

# write_parquet -> ENUM

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name    r_type       type type_length repetition_type converted_type
      1 schema      <NA>       <NA>          NA            <NA>           <NA>
      2      d character BYTE_ARRAY          NA        OPTIONAL           ENUM
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
      4   <NA>

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name    r_type       type type_length repetition_type converted_type
      1 schema    factor       <NA>          NA            <NA>           <NA>
      2      d character BYTE_ARRAY          NA        OPTIONAL           ENUM
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
      4   <NA>

# write_parquet -> DECIMAL INT32

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL        DECIMAL DECIMAL,....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA     0         3       NA
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
      12 NA

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too large for INT32 DECIMAL with precision 3, scale 0: 1000

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too small for INT32 DECIMAL with precision 3, scale 0: -1000

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL        DECIMAL DECIMAL,....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA     2         3       NA
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
      12 NA

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too large for INT32 DECIMAL with precision 3, scale 2: 10

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too small for INT32 DECIMAL with precision 3, scale 2: -10

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL        DECIMAL DECIMAL,....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA     1         3       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
            d
      1  -2.5
      2  -2.0
      3  -1.5
      4  -1.0
      5  -0.5
      6   0.0
      7   0.5
      8   1.0
      9   1.5
      10  2.0
      11  2.5
      12   NA

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too large for INT32 DECIMAL with precision 3, scale 1: 100.000000

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too small for INT32 DECIMAL with precision 3, scale 1: -100.000000

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL        DECIMAL DECIMAL,....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA     2         3       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
            d
      1  -2.5
      2  -2.0
      3  -1.5
      4  -1.0
      5  -0.5
      6   0.0
      7   0.5
      8   1.0
      9   1.5
      10  2.0
      11  2.5
      12   NA

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too large for INT32 DECIMAL with precision 3, scale 2: 10.000000

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too small for INT32 DECIMAL with precision 3, scale 2: -10.000000

# write_parquet -> DECIMAL INT64

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        OPTIONAL        DECIMAL DECIMAL,....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA     0         3       NA
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
      12 NA

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too large for INT64 DECIMAL with precision 3, scale 0: 1000

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too small for INT64 DECIMAL with precision 3, scale 0: -1000

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        OPTIONAL        DECIMAL DECIMAL,....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA     2         3       NA
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
      12 NA

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too large for INT64 DECIMAL with precision 3, scale 2: 10

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too small for INT64 DECIMAL with precision 3, scale 2: -10

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        OPTIONAL        DECIMAL DECIMAL,....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA     1         3       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
            d
      1  -2.5
      2  -2.0
      3  -1.5
      4  -1.0
      5  -0.5
      6   0.0
      7   0.5
      8   1.0
      9   1.5
      10  2.0
      11  2.5
      12   NA

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too large for INT64 DECIMAL with precision 3, scale 1: 100.000000

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too small for INT64 DECIMAL with precision 3, scale 1: -100.000000

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        OPTIONAL        DECIMAL DECIMAL,....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA     2         3       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
            d
      1  -2.5
      2  -2.0
      3  -1.5
      4  -1.0
      5  -0.5
      6   0.0
      7   0.5
      8   1.0
      9   1.5
      10  2.0
      11  2.5
      12   NA

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too large for INT64 DECIMAL with precision 3, scale 2: 10.000000

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Value too small for INT64 DECIMAL with precision 3, scale 2: -10.000000

# smaller integers

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL          INT_8 INT, 8, TRUE
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
      12 NA

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT_8"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for INT with bit width 8: 128 at column 1, row 2:

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT_8"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too small for INT with bit width 8: -129 at column 1, row 3:

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL         INT_16 INT, 16,....
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
      12 NA

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT_16"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for INT with bit width 16: 32768 at column 1, row 2:

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT_16"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too small for INT with bit width 16: -32769 at column 1, row 3:

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL         UINT_8 INT, 8, ....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  0
      2  1
      3  2
      4  3
      5  4
      6  5
      7 NA

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UINT_8"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for UINT with bit width 8: 256 at column 1, row 2:

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UINT_8"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too small for UINT with bit width 8: -1 at column 1, row 2:

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL        UINT_16 INT, 16,....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  0
      2  1
      3  2
      4  3
      5  4
      6  5
      7 NA

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UINT_16"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for UINT with bit width 16: 65536 at column 1, row 1:

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UINT_16"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too small for UINT with bit width 16: -1 at column 1, row 2:

# double to smaller integers

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL          INT_8 INT, 8, TRUE
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
      12 NA

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT_8"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for INT with bit width 8: 128.000000 at column 1, row 2:

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT_8"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too small for INT with bit width 8: -129.000000 at column 1, row 3:

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL         INT_16 INT, 16,....
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
      12 NA

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT_16"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for INT with bit width 16: 32768.000000 at column 1, row 2:

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT_16"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too small for INT with bit width 16: -32769.000000 at column 1, row 3:

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL         INT_32 INT, 32,....
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
      12 NA

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT_32"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for INT with bit width 32: 2147483648.000000 at column 1, row 2:

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT_32"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too small for INT with bit width 32: -2147483649.000000 at column 1, row 2:

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL         UINT_8 INT, 8, ....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  0
      2  1
      3  2
      4  3
      5  4
      6  5
      7 NA

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UINT_8"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for INT with bit width 8: 256.000000 at column 1, row 2.

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UINT_8"))
    Condition
      Error in `write_parquet()`:
      ! Negative values are not allowed in unsigned INT column:-1.000000 at column 1, row 2.

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL        UINT_16 INT, 16,....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  0
      2  1
      3  2
      4  3
      5  4
      6  5
      7 NA

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UINT_16"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for INT with bit width 16: 65536.000000 at column 1, row 2.

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UINT_16"))
    Condition
      Error in `write_parquet()`:
      ! Negative values are not allowed in unsigned INT column:-1.000000 at column 1, row 2.

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2      d integer INT32          NA        OPTIONAL        UINT_32 INT, 32,....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  0
      2  1
      3  2
      4  3
      5  4
      6  5
      7 NA

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UINT_32"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for INT with bit width 32: 4294967296.000000 at column 1, row 2.

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UINT_32"))
    Condition
      Error in `write_parquet()`:
      ! Negative values are not allowed in unsigned INT column:-1.000000 at column 1, row 2.

# double to INT(64, *)

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        OPTIONAL         INT_64 INT, 64,....
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
      12 NA

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT_64"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for INT with bit width 64: 18446744073709551616.000000 at column 1, row 2.

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT_64"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too small for INT with bit width 64: -18446744073709551616.000000 at column 1, row 2.

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type  type type_length repetition_type converted_type logical_type
      1 schema   <NA>  <NA>          NA            <NA>           <NA>             
      2      d double INT64          NA        OPTIONAL        UINT_64 INT, 64,....
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  0
      2  1
      3  2
      4  3
      5  4
      6  5
      7 NA

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UINT_64"))
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for unsigned INT with bit width 64: 36893488147419103232.000000 at column 1, row 2.

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UINT_64"))
    Condition
      Error in `write_parquet()`:
      ! Negative values are not allowed in unsigned INT column:-1.000000 at column 1, row 2.

# JSON

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type       type type_length repetition_type converted_type
      1 schema   <NA>       <NA>          NA            <NA>           <NA>
      2      d    raw BYTE_ARRAY          NA        OPTIONAL           JSON
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2         JSON           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
             d
      1    foo
      2    bar
      3 foobar
      4   <NA>

# UUID

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name    r_type                 type type_length repetition_type
      1 schema      <NA>                 <NA>          NA            <NA>
      2      d character FIXED_LEN_BYTE_ARRAY          16        OPTIONAL
        converted_type logical_type num_children scale precision field_id
      1           <NA>                         1    NA        NA       NA
      2           <NA>         UUID           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                           d
      1 00112233-4455-6677-8899-aabbccddeeff
      2 00112233-4455-6677-8899-aabbccddeeff
      3 00112233-4455-6677-8899-aabbccddeeff
      4                                 <NA>

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("UUID"))
    Condition
      Error in `write_parquet()`:
      ! Invalid UUID value in column 1, row 2

# FLOAT16

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type                 type type_length repetition_type converted_type
      1 schema   <NA>                 <NA>          NA            <NA>           <NA>
      2      c    raw FIXED_LEN_BYTE_ARRAY           2        OPTIONAL           <NA>
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2      FLOAT16           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
            c
      1   0.0
      2   1.0
      3   2.0
      4    NA
      5  -1.0
      6   NaN
      7  -2.0
      8  -Inf
      9   Inf
      10  0.5

# NaN

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type   type type_length repetition_type converted_type logical_type
      1 schema   <NA>   <NA>          NA            <NA>           <NA>             
      2      d double DOUBLE          NA        REQUIRED           <NA>             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   2
      3 NaN
      4   4
      5   5

# list of RAW to BYTE_ARRAY

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type       type type_length repetition_type converted_type
      1 schema   <NA>       <NA>          NA            <NA>           <NA>
      2      d    raw BYTE_ARRAY          NA        REQUIRED           <NA>
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2                        NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
                             d
      1             66, 6f, 6f
      2             62, 61, 72
      3 66, 6f, 6f, 62, 61, 72

---

    Code
      as.data.frame(read_parquet_schema(tmp)[, -1])
    Output
          name r_type       type type_length repetition_type converted_type
      1 schema   <NA>       <NA>          NA            <NA>           <NA>
      2      d    raw BYTE_ARRAY          NA        OPTIONAL           <NA>
        logical_type num_children scale precision field_id
      1                         1    NA        NA       NA
      2                        NA    NA        NA       NA
    Code
      as.data.frame(read_parquet(tmp))
    Output
                             d
      1             66, 6f, 6f
      2                   NULL
      3             62, 61, 72
      4 66, 6f, 6f, 62, 61, 72
      5                   NULL

