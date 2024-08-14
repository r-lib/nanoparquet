# read factors, marked by Arrow

    Code
      as.data.frame(res[1:5, ])
    Output
                      nam  mpg cyl disp  hp drat    wt  qsec vs am gear carb large
      1         Mazda RX4 21.0   6  160 110 3.90 2.620 16.46  0  1    4    4  TRUE
      2     Mazda RX4 Wag 21.0   6  160 110 3.90 2.875 17.02  0  1    4    4  TRUE
      3        Datsun 710 22.8   4  108  93 3.85 2.320 18.61  1  1    4    1 FALSE
      4    Hornet 4 Drive 21.4   6  258 110 3.08 3.215 19.44  1  0    3    1  TRUE
      5 Hornet Sportabout 18.7   8  360 175 3.15 3.440 17.02  0  0    3    2  TRUE
        fac
      1   m
      2   m
      3   d
      4   h
      5   h
    Code
      sapply(res, class)
    Output
              nam         mpg         cyl        disp          hp        drat 
      "character"   "numeric"   "integer"   "numeric"   "numeric"   "numeric" 
               wt        qsec          vs          am        gear        carb 
        "numeric"   "numeric"   "numeric"   "numeric"   "numeric"   "numeric" 
            large         fac 
        "logical"    "factor" 

# Can't parse Arrow schema

    Code
      arrow_find_special(base64_encode("foobar"), "myfile")
    Condition
      Warning in `value[[3L]]()`:
      Failed to parse Arrow schema from parquet file at 'myfile'
    Output
      list()

# read hms in MICROS

    Code
      as.data.frame(read_parquet2(pf))
    Output
              tt
      1 14:30:00
      2 11:35:00
      3 01:59:00

# read difftime

    Code
      as.data.frame(d2)
    Output
               h
      1 600 secs

# read GZIP compressed files

    Code
      as.data.frame(read_parquet2(pf))
    Output
                         nam  mpg cyl  disp  hp drat    wt  qsec vs am gear carb
      1                 <NA> 21.0   6 160.0 110 3.90 2.620 16.46  0  1    4    4
      2        Mazda RX4 Wag   NA   6 160.0 110 3.90 2.875 17.02  0  1    4    4
      3           Datsun 710 22.8  NA 108.0  93 3.85 2.320 18.61  1  1    4    1
      4       Hornet 4 Drive 21.4   6    NA 110 3.08 3.215 19.44  1  0    3    1
      5    Hornet Sportabout 18.7   8 360.0  NA 3.15 3.440 17.02  0  0    3    2
      6              Valiant 18.1   6 225.0 105   NA 3.460 20.22  1  0    3    1
      7           Duster 360 14.3   8 360.0 245 3.21    NA 15.84  0  0    3    4
      8            Merc 240D 24.4   4 146.7  62 3.69 3.190    NA  1  0    4    2
      9             Merc 230 22.8   4 140.8  95 3.92 3.150 22.90 NA  0    4    2
      10            Merc 280 19.2   6 167.6 123 3.92 3.440 18.30  1 NA    4    4
      11           Merc 280C 17.8   6 167.6 123 3.92 3.440 18.90  1  0   NA    4
      12          Merc 450SE 16.4   8 275.8 180 3.07 4.070 17.40  0  0    3   NA
      13          Merc 450SL 17.3   8 275.8 180 3.07 3.730 17.60  0  0    3    3
      14         Merc 450SLC 15.2   8 275.8 180 3.07 3.780 18.00  0  0    3    3
      15  Cadillac Fleetwood 10.4   8 472.0 205 2.93 5.250 17.98  0  0    3    4
      16 Lincoln Continental 10.4   8 460.0 215 3.00 5.424 17.82  0  0    3    4
      17   Chrysler Imperial 14.7   8 440.0 230 3.23 5.345 17.42  0  0    3    4
      18            Fiat 128 32.4   4  78.7  66 4.08 2.200 19.47  1  1    4    1
      19         Honda Civic 30.4   4  75.7  52 4.93 1.615 18.52  1  1    4    2
      20      Toyota Corolla 33.9   4  71.1  65 4.22 1.835 19.90  1  1    4    1
      21       Toyota Corona 21.5   4 120.1  97 3.70 2.465 20.01  1  0    3    1
      22    Dodge Challenger 15.5   8 318.0 150 2.76 3.520 16.87  0  0    3    2
      23         AMC Javelin 15.2   8 304.0 150 3.15 3.435 17.30  0  0    3    2
      24          Camaro Z28 13.3   8 350.0 245 3.73 3.840 15.41  0  0    3    4
      25    Pontiac Firebird 19.2   8 400.0 175 3.08 3.845 17.05  0  0    3    2
      26           Fiat X1-9 27.3   4  79.0  66 4.08 1.935 18.90  1  1    4    1
      27       Porsche 914-2 26.0   4 120.3  91 4.43 2.140 16.70  0  1    5    2
      28        Lotus Europa 30.4   4  95.1 113 3.77 1.513 16.90  1  1    5    2
      29      Ford Pantera L 15.8   8 351.0 264 4.22 3.170 14.50  0  1    5    4
      30        Ferrari Dino 19.7   6 145.0 175 3.62 2.770 15.50  0  1    5    6
      31       Maserati Bora 15.0   8 301.0 335 3.54 3.570 14.60  0  1    5    8
      32          Volvo 142E 21.4   4 121.0 109 4.11 2.780 18.60  1  1    4    2
         large
      1   TRUE
      2   TRUE
      3  FALSE
      4   TRUE
      5   TRUE
      6   TRUE
      7   TRUE
      8  FALSE
      9  FALSE
      10  TRUE
      11  TRUE
      12  TRUE
      13    NA
      14  TRUE
      15  TRUE
      16  TRUE
      17  TRUE
      18 FALSE
      19 FALSE
      20 FALSE
      21 FALSE
      22  TRUE
      23  TRUE
      24  TRUE
      25  TRUE
      26 FALSE
      27 FALSE
      28 FALSE
      29  TRUE
      30  TRUE
      31  TRUE
      32 FALSE

# V2 data pages

    Code
      as.data.frame(read_parquet2(pf))
    Output
        FirstName                                       Data
      1      John 48, 65, 6c, 6c, 6f, 20, 57, 6f, 72, 6c, 64
      2      John 48, 65, 6c, 6c, 6f, 20, 57, 6f, 72, 6c, 64
      3      John 48, 65, 6c, 6c, 6f, 20, 57, 6f, 72, 6c, 64
      4      John 48, 65, 6c, 6c, 6f, 20, 57, 6f, 72, 6c, 64
      5      John 48, 65, 6c, 6c, 6f, 20, 57, 6f, 72, 6c, 64

# Tricky V2 data page

    Code
      as.data.frame(read_parquet2(pf))
    Output
         datatype_boolean
      1              TRUE
      2             FALSE
      3                NA
      4              TRUE
      5              TRUE
      6             FALSE
      7             FALSE
      8              TRUE
      9              TRUE
      10             TRUE
      11            FALSE
      12            FALSE
      13             TRUE
      14             TRUE
      15            FALSE
      16               NA
      17             TRUE
      18             TRUE
      19            FALSE
      20            FALSE
      21             TRUE
      22             TRUE
      23            FALSE
      24               NA
      25             TRUE
      26             TRUE
      27            FALSE
      28            FALSE
      29             TRUE
      30             TRUE
      31             TRUE
      32            FALSE
      33            FALSE
      34            FALSE
      35            FALSE
      36             TRUE
      37             TRUE
      38            FALSE
      39               NA
      40             TRUE
      41             TRUE
      42            FALSE
      43            FALSE
      44             TRUE
      45             TRUE
      46             TRUE
      47            FALSE
      48            FALSE
      49               NA
      50             TRUE
      51             TRUE
      52            FALSE
      53            FALSE
      54             TRUE
      55             TRUE
      56             TRUE
      57            FALSE
      58             TRUE
      59             TRUE
      60            FALSE
      61               NA
      62             TRUE
      63             TRUE
      64            FALSE
      65            FALSE
      66             TRUE
      67             TRUE
      68             TRUE

# DELTA_BIANRY_PACKED encoding

    Code
      read_parquet_metadata(pf)$column_chunks$encodings
    Output
      [[1]]
      [1] "RLE"                 "DELTA_BINARY_PACKED"
      
      [[2]]
      [1] "RLE"                 "DELTA_BINARY_PACKED"
      
    Code
      read_parquet2(pf)
    Output
      # A data frame: 20 x 2
             x     y
         <int> <int>
       1     1   -10
       2     2    -9
       3     3    -8
       4     4    -7
       5     5    -6
       6     6    -5
       7     7    -4
       8     8    -3
       9     9    -2
      10    10    -1
      11  1001    11
      12  1002    12
      13  1003    13
      14  1004    14
      15  1005    15
      16  1006    16
      17  1007    17
      18  1008    18
      19  1009    19
      20  1010    20

---

    Code
      read_parquet_metadata(pf2)$column_chunks$encodings
    Output
      [[1]]
      [1] "RLE"                 "DELTA_BINARY_PACKED"
      
      [[2]]
      [1] "RLE"                 "DELTA_BINARY_PACKED"
      
    Code
      read_parquet2(pf2)
    Output
      # A data frame: 20 x 2
             x     y
         <int> <int>
       1     1   -10
       2     2    NA
       3     3    -8
       4     4    -7
       5     5    -6
       6    NA    -5
       7    NA    -4
       8     8    -3
       9     9    -2
      10    10    -1
      11  1001    11
      12  1002    12
      13    NA    NA
      14  1004    14
      15  1005    15
      16    NA    16
      17  1007    17
      18  1008    NA
      19    NA    19
      20  1010    20

---

    Code
      read_parquet_metadata(pf3)$column_chunks$encodings
    Output
      [[1]]
      [1] "RLE"                 "DELTA_BINARY_PACKED"
      
    Code
      read_parquet2(pf3)
    Output
      # A data frame: 7 x 1
            x
        <dbl>
      1  -100
      2    -1
      3     0
      4     2
      5     4
      6     5
      7   100

# UUID columns

    Code
      as.data.frame(read_parquet2(pf))
    Output
                                            u
      1                                  <NA>
      2                                  <NA>
      3                                  <NA>
      4                                  <NA>
      5  ffffffff-ffff-ffff-ffff-ffffffffffff
      6  ffffffff-ffff-ffff-ffff-ffffffffffff
      7  a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
      8  a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
      9  a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
      10 a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
      11 8fffffff-ffff-ffff-ffff-ffffffffffff
      12 8fffffff-ffff-ffff-ffff-ffffffffffff
      13 8fffffff-ffff-ffff-8fff-ffffffffffff
      14 8fffffff-ffff-ffff-8fff-ffffffffffff
      15 8fffffff-ffff-ffff-8000-000000000000
      16 8fffffff-ffff-ffff-8000-000000000000
      17 8fffffff-ffff-ffff-0000-000000000000
      18 8fffffff-ffff-ffff-0000-000000000000
      19 80000000-0000-0000-ffff-ffffffffffff
      20 80000000-0000-0000-ffff-ffffffffffff
      21 80000000-0000-0000-8fff-ffffffffffff
      22 80000000-0000-0000-8fff-ffffffffffff
      23 80000000-0000-0000-8000-000000000000
      24 80000000-0000-0000-8000-000000000000
      25 80000000-0000-0000-0000-000000000000
      26 80000000-0000-0000-0000-000000000000
      27 47183823-2574-4bfd-b411-99ed177d3e43
      28 47183823-2574-4bfd-b411-99ed177d3e43
      29 47183823-2574-4bfd-b411-99ed177d3e43
      30 47183823-2574-4bfd-b411-99ed177d3e43
      31 10203040-5060-7080-0102-030405060708
      32 10203040-5060-7080-0102-030405060708
      33 10203040-5060-7080-0102-030405060708
      34 10203040-5060-7080-0102-030405060708
      35 00000000-0000-0000-8000-000000000001
      36 00000000-0000-0000-8000-000000000001
      37 00000000-0000-0000-0000-000000000001
      38 00000000-0000-0000-0000-000000000001
      39 00000000-0000-0000-0000-000000000000
      40 00000000-0000-0000-0000-000000000000

# DELTA_LENGTH_BYTE_ARRAY encoding

    Code
      as.data.frame(dlba)[1:10, ]
    Output
       [1] "apple_banana_mango0"  "apple_banana_mango1"  "apple_banana_mango4" 
       [4] "apple_banana_mango9"  "apple_banana_mango16" "apple_banana_mango25"
       [7] "apple_banana_mango36" "apple_banana_mango49" "apple_banana_mango64"
      [10] "apple_banana_mango81"
    Code
      rle(nchar(dlba$FRUIT))
    Output
      Run Length Encoding
        lengths: int [1:6] 4 6 22 68 217 683
        values : int [1:6] 19 20 21 22 23 24

# DELTA_BYTE_ARRAY encoding

    Code
      as.data.frame(dba)[1:5, ]
    Output
           c_customer_id c_salutation c_first_name c_last_name c_preferred_cust_flag
      1 AAAAAAAAIODAAAAA          Sir         Mark      Bailey                     N
      2 AAAAAAAAHODAAAAA         Mrs.         Lisa       Clark                     Y
      3 AAAAAAAAGODAAAAA          Ms.       Evelyn      Joyner                     N
      4 AAAAAAAAFODAAAAA          Sir       Harvey        <NA>                     N
      5 AAAAAAAAEODAAAAA          Dr.        Chris       Davis                     Y
        c_birth_country c_login                  c_email_address c_last_review_date
      1         MOROCCO    <NA>   Mark.Bailey@rg9qCNVJ0s7qeY.com            2452443
      2           ITALY    <NA>        Lisa.Clark@goPYS4tMB0.org            2452646
      3          TUVALU    <NA>      Evelyn.Joyner@ialYx1zLN.edu            2452439
      4            <NA>    <NA> Harvey.Stanford@sl59JiHqrp8X.org            2452632
      5         ALBANIA    <NA>            Chris.Davis@k6S3Q.com            2452570

# BYTE_STREAM_SPLIT encoding

    Code
      as.data.frame(bss)[1:5, ]
    Output
           floats    doubles nullable_floats
      1  27.39234  -4.002415              NA
      2 -46.04266 -53.525416              NA
      3 -91.80530  60.376116        70.37518
      4 -96.69447  84.706032       -72.21366
      5  62.65405 -46.773946        40.75715

# More BYTE_STREAM_SPLIT

    Code
      as.data.frame(bss)[1:5, ]
    Output
        float16_plain float16_byte_stream_split float_plain float_byte_stream_split
      1     10.304688                 10.304688   10.337575               10.337575
      2      8.960938                  8.960938   11.407482               11.407482
      3     10.750000                 10.750000   10.090585               10.090585
      4     10.937500                 10.937500   10.643939               10.643939
      5      8.046875                  8.046875    7.949828                7.949828
        double_plain double_byte_stream_split int32_plain int32_byte_stream_split
      1     9.820389                 9.820389       24191                   24191
      2    10.196776                10.196776       41157                   41157
      3    10.820528                10.820528        7403                    7403
      4     9.606259                 9.606259       79368                   79368
      5    10.521167                10.521167       64983                   64983
        int64_plain int64_byte_stream_split        flba5_plain
      1 2.93650e+11             2.93650e+11 30, 33, 37, 39, 35
      2 4.10790e+10             4.10790e+10 30, 30, 33, 36, 33
      3 5.12480e+10             5.12480e+10 30, 31, 30, 33, 38
      4 2.46066e+11             2.46066e+11 31, 31, 39, 31, 34
      5 5.72141e+11             5.72141e+11 30, 33, 31, 32, 35
        flba5_byte_stream_split decimal_plain decimal_byte_stream_split
      1      30, 33, 37, 39, 35      251679.1                  251679.1
      2      30, 30, 33, 36, 33      234932.3                  234932.3
      3      30, 31, 30, 33, 38      268491.8                  268491.8
      4      31, 31, 39, 31, 34      234895.9                  234895.9
      5      30, 33, 31, 32, 35      218165.6                  218165.6

# DECIMAL in INT32, INT64

    Code
      as.data.frame(read_parquet2(pf))
    Output
         value
      1      1
      2      2
      3      3
      4      4
      5      5
      6      6
      7      7
      8      8
      9      9
      10    10
      11    11
      12    12
      13    13
      14    14
      15    15
      16    16
      17    17
      18    18
      19    19
      20    20
      21    21
      22    22
      23    23
      24    24

---

    Code
      as.data.frame(read_parquet2(pf))
    Output
         value
      1      1
      2      2
      3      3
      4      4
      5      5
      6      6
      7      7
      8      8
      9      9
      10    10
      11    11
      12    12
      13    13
      14    14
      15    15
      16    16
      17    17
      18    18
      19    19
      20    20
      21    21
      22    22
      23    23
      24    24

# FLOAT16

    Code
      as.data.frame(read_parquet_schema(pf))
    Output
                                     file_name   name r_type                 type
      1 data/float16_nonzeros_and_nans.parquet schema   <NA>                 <NA>
      2 data/float16_nonzeros_and_nans.parquet      x    raw FIXED_LEN_BYTE_ARRAY
        type_length repetition_type converted_type logical_type num_children scale
      1          NA        REQUIRED           <NA>                         1    NA
      2           2        OPTIONAL           <NA>      FLOAT16           NA    NA
        precision field_id
      1        NA       NA
      2        NA       NA
    Code
      as.data.frame(read_parquet2(pf))
    Output
          x
      1  NA
      2   1
      3  -2
      4 NaN
      5   0
      6  -1
      7   0
      8   2

