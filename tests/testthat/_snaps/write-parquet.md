# factors are written as strings

    Code
      as.data.frame(parquet_schema(tmp))[c("name", "type", "converted_type",
        "logical_type")]
    Output
           name       type converted_type logical_type
      1  schema       <NA>           <NA>             
      2     nam BYTE_ARRAY           UTF8       STRING
      3     mpg     DOUBLE           <NA>             
      4     cyl      INT32         INT_32 INT, 32,....
      5    disp     DOUBLE           <NA>             
      6      hp     DOUBLE           <NA>             
      7    drat     DOUBLE           <NA>             
      8      wt     DOUBLE           <NA>             
      9    qsec     DOUBLE           <NA>             
      10     vs     DOUBLE           <NA>             
      11     am     DOUBLE           <NA>             
      12   gear     DOUBLE           <NA>             
      13   carb     DOUBLE           <NA>             
      14  large    BOOLEAN           <NA>             
      15    fac BYTE_ARRAY           UTF8       STRING

# round trip with pandas/pyarrow

    Code
      writeLines(pyout$stdout)
    Output
                          nam   mpg  cyl   disp     hp  drat     wt   qsec   vs   am  gear  carb  large
      0             Mazda RX4  21.0    6  160.0  110.0  3.90  2.620  16.46  0.0  1.0   4.0   4.0   True
      1         Mazda RX4 Wag  21.0    6  160.0  110.0  3.90  2.875  17.02  0.0  1.0   4.0   4.0   True
      2            Datsun 710  22.8    4  108.0   93.0  3.85  2.320  18.61  1.0  1.0   4.0   1.0  False
      3        Hornet 4 Drive  21.4    6  258.0  110.0  3.08  3.215  19.44  1.0  0.0   3.0   1.0   True
      4     Hornet Sportabout  18.7    8  360.0  175.0  3.15  3.440  17.02  0.0  0.0   3.0   2.0   True
      5               Valiant  18.1    6  225.0  105.0  2.76  3.460  20.22  1.0  0.0   3.0   1.0   True
      6            Duster 360  14.3    8  360.0  245.0  3.21  3.570  15.84  0.0  0.0   3.0   4.0   True
      7             Merc 240D  24.4    4  146.7   62.0  3.69  3.190  20.00  1.0  0.0   4.0   2.0  False
      8              Merc 230  22.8    4  140.8   95.0  3.92  3.150  22.90  1.0  0.0   4.0   2.0  False
      9              Merc 280  19.2    6  167.6  123.0  3.92  3.440  18.30  1.0  0.0   4.0   4.0   True
      10            Merc 280C  17.8    6  167.6  123.0  3.92  3.440  18.90  1.0  0.0   4.0   4.0   True
      11           Merc 450SE  16.4    8  275.8  180.0  3.07  4.070  17.40  0.0  0.0   3.0   3.0   True
      12           Merc 450SL  17.3    8  275.8  180.0  3.07  3.730  17.60  0.0  0.0   3.0   3.0   True
      13          Merc 450SLC  15.2    8  275.8  180.0  3.07  3.780  18.00  0.0  0.0   3.0   3.0   True
      14   Cadillac Fleetwood  10.4    8  472.0  205.0  2.93  5.250  17.98  0.0  0.0   3.0   4.0   True
      15  Lincoln Continental  10.4    8  460.0  215.0  3.00  5.424  17.82  0.0  0.0   3.0   4.0   True
      16    Chrysler Imperial  14.7    8  440.0  230.0  3.23  5.345  17.42  0.0  0.0   3.0   4.0   True
      17             Fiat 128  32.4    4   78.7   66.0  4.08  2.200  19.47  1.0  1.0   4.0   1.0  False
      18          Honda Civic  30.4    4   75.7   52.0  4.93  1.615  18.52  1.0  1.0   4.0   2.0  False
      19       Toyota Corolla  33.9    4   71.1   65.0  4.22  1.835  19.90  1.0  1.0   4.0   1.0  False
      20        Toyota Corona  21.5    4  120.1   97.0  3.70  2.465  20.01  1.0  0.0   3.0   1.0  False
      21     Dodge Challenger  15.5    8  318.0  150.0  2.76  3.520  16.87  0.0  0.0   3.0   2.0   True
      22          AMC Javelin  15.2    8  304.0  150.0  3.15  3.435  17.30  0.0  0.0   3.0   2.0   True
      23           Camaro Z28  13.3    8  350.0  245.0  3.73  3.840  15.41  0.0  0.0   3.0   4.0   True
      24     Pontiac Firebird  19.2    8  400.0  175.0  3.08  3.845  17.05  0.0  0.0   3.0   2.0   True
      25            Fiat X1-9  27.3    4   79.0   66.0  4.08  1.935  18.90  1.0  1.0   4.0   1.0  False
      26        Porsche 914-2  26.0    4  120.3   91.0  4.43  2.140  16.70  0.0  1.0   5.0   2.0  False
      27         Lotus Europa  30.4    4   95.1  113.0  3.77  1.513  16.90  1.0  1.0   5.0   2.0  False
      28       Ford Pantera L  15.8    8  351.0  264.0  4.22  3.170  14.50  0.0  1.0   5.0   4.0   True
      29         Ferrari Dino  19.7    6  145.0  175.0  3.62  2.770  15.50  0.0  1.0   5.0   6.0   True
      30        Maserati Bora  15.0    8  301.0  335.0  3.54  3.570  14.60  0.0  1.0   5.0   8.0   True
      31           Volvo 142E  21.4    4  121.0  109.0  4.11  2.780  18.60  1.0  1.0   4.0   2.0  False
      nam       object
      mpg      float64
      cyl        int32
      disp     float64
      hp       float64
      drat     float64
      wt       float64
      qsec     float64
      vs       float64
      am       float64
      gear     float64
      carb     float64
      large       bool
      dtype: object
      

# errors

    Code
      write_parquet(mt, tmp)
    Condition
      Error in `encode_arrow_schema_r()`:
      ! Unsuppoted types when writing Parquet file: list

---

    Code
      write_parquet(mt, 1:10)
    Condition
      Error in `path.expand()`:
      ! invalid 'path' argument

---

    Code
      write_parquet(mt2, tmp, metadata = "bad")
    Condition
      Error in `write_parquet()`:
      ! length(names(metadata)) == length(metadata) is not TRUE

---

    Code
      write_parquet(mt2, tmp, metadata = mtcars)
    Condition
      Error in `write_parquet()`:
      ! ncol(metadata) == 2 is not TRUE

# writing metadata

    Code
      as.data.frame(kvm)[1, ]
    Output
        key value
      1 foo   bar

# strings are converted to UTF-8

    Code
      charToRaw(utf8)
    Output
       [1] 43 73 c3 a1 72 64 69 20 47 c3 a1 62 6f 72
    Code
      charToRaw(df$utf8)
    Output
       [1] 43 73 c3 a1 72 64 69 20 47 c3 a1 62 6f 72
    Code
      charToRaw(df$latin1)
    Output
       [1] 43 73 e1 72 64 69 20 47 e1 62 6f 72

---

    Code
      charToRaw(utf8)
    Output
       [1] 43 73 c3 a1 72 64 69 20 47 c3 a1 62 6f 72
    Code
      charToRaw(df2$utf8)
    Output
       [1] 43 73 c3 a1 72 64 69 20 47 c3 a1 62 6f 72
    Code
      charToRaw(df2$latin1)
    Output
       [1] 43 73 c3 a1 72 64 69 20 47 c3 a1 62 6f 72

# REQ PLAIN

    Code
      read_parquet_page(tmp, pgs$page_header_offset[1])$data
    Output
       [1] 01 00 00 00 02 00 00 00 02 00 00 00 02 00 00 00 03 00 00 00 03 00 00 00 03
      [26] 00 00 00 03 00 00 00
    Code
      read_parquet_page(tmp, pgs$page_header_offset[2])$data
    Output
       [1] 00 00 00 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00
      [26] 00 00 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00 00
      [51] 00 00 00 00 00 40 00 00 00 00 00 00 24 40
    Code
      read_parquet_page(tmp, pgs$page_header_offset[3])$data
    Output
      [1] d1
    Code
      read_parquet_page(tmp, pgs$page_header_offset[4])$data
    Output
       [1] 01 00 00 00 41 01 00 00 00 41 01 00 00 00 41 01 00 00 00 42 01 00 00 00 43
      [26] 01 00 00 00 44 01 00 00 00 44 01 00 00 00 44

---

    Code
      read_parquet_page(tmp, pgs$page_header_offset[1])$data
    Output
       [1] 01 00 00 00 02 00 00 00 02 00 00 00 02 00 00 00 03 00 00 00 03 00 00 00 03
      [26] 00 00 00 03 00 00 00
    Code
      read_parquet_page(tmp, pgs$page_header_offset[2])$data
    Output
       [1] 00 00 00 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00
      [26] 00 00 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00 00
      [51] 00 00 00 00 00 40 00 00 00 00 00 00 24 40
    Code
      read_parquet_page(tmp, pgs$page_header_offset[3])$data
    Output
      [1] d1
    Code
      read_parquet_page(tmp, pgs$page_header_offset[4])$data
    Output
       [1] 01 00 00 00 41 01 00 00 00 41 01 00 00 00 41 01 00 00 00 42 01 00 00 00 43
      [26] 01 00 00 00 44 01 00 00 00 44 01 00 00 00 44

# OPT PLAIN

    Code
      data <- print(read_parquet_page(tmp, pgs$page_header_offset[1])$data)
    Output
       [1] 02 00 00 00 03 e5 01 00 00 00 02 00 00 00 03 00 00 00 03 00 00 00 03 00 00
      [26] 00
    Code
      readBin(data[-(1:6)], "integer", sum(!is.na(d$i)))
    Output
      [1] 1 2 3 3 3
    Code
      data <- print(read_parquet_page(tmp, pgs$page_header_offset[2])$data)
    Output
       [1] 02 00 00 00 03 3f 00 00 00 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00 00 00
      [26] 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00 00 00 00
      [51] 00 00 f0 3f
    Code
      readBin(data[-(1:6)], "double", sum(!is.na(d$r)))
    Output
      [1] 1 1 1 1 1 1
    Code
      data <- print(read_parquet_page(tmp, pgs$page_header_offset[3])$data)
    Output
      [1] 02 00 00 00 03 73 15
    Code
      data <- print(read_parquet_page(tmp, pgs$page_header_offset[4])$data)
    Output
       [1] 02 00 00 00 03 5c 01 00 00 00 41 01 00 00 00 42 01 00 00 00 43 01 00 00 00
      [26] 44

---

    Code
      data <- print(read_parquet_page(tmp, pgs$page_header_offset[1])$data)
    Output
       [1] 02 00 00 00 03 e5 01 00 00 00 02 00 00 00 03 00 00 00 03 00 00 00 03 00 00
      [26] 00
    Code
      readBin(data[-(1:6)], "integer", sum(!is.na(d$i)))
    Output
      [1] 1 2 3 3 3
    Code
      data <- print(read_parquet_page(tmp, pgs$page_header_offset[2])$data)
    Output
       [1] 02 00 00 00 03 3f 00 00 00 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00 00 00
      [26] 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00 00 00 00 00 00 f0 3f 00 00 00 00
      [51] 00 00 f0 3f
    Code
      readBin(data[-(1:6)], "double", sum(!is.na(d$r)))
    Output
      [1] 1 1 1 1 1 1
    Code
      data <- print(read_parquet_page(tmp, pgs$page_header_offset[3])$data)
    Output
      [1] 02 00 00 00 03 73 15
    Code
      data <- print(read_parquet_page(tmp, pgs$page_header_offset[4])$data)
    Output
       [1] 02 00 00 00 03 5c 01 00 00 00 41 01 00 00 00 42 01 00 00 00 43 01 00 00 00
      [26] 44

# REQ RLE_DICT

    Code
      read_parquet_page(tmp, pgs$page_header_offset[1])$data
    Output
       [1] 01 00 00 00 41 01 00 00 00 42 01 00 00 00 43 01 00 00 00 44 01 00 00 00 45
    Code
      data <- print(read_parquet_page(tmp, pgs$page_header_offset[2])$data)
    Output
      [1] 03 03 40 22 8d
    Code
      rle_decode_int(data[-1], bit_width = as.integer(data[1]), nrow(d))
    Output
      [1] 0 0 1 1 2 2 3 4

---

    Code
      read_parquet_page(tmp, pgs$page_header_offset[1])$data
    Output
       [1] 01 00 00 00 41 01 00 00 00 42 01 00 00 00 43 01 00 00 00 44 01 00 00 00 45
    Code
      read_parquet_page(tmp, pgs$page_header_offset[2])$data
    Output
      [1] 03 03 40 22 8d

# OPT RLE_DICT

    Code
      read_parquet_page(tmp, pgs$page_header_offset[1])$data
    Output
       [1] 01 00 00 00 41 01 00 00 00 42 01 00 00 00 43 01 00 00 00 44 01 00 00 00 45
    Code
      data <- print(read_parquet_page(tmp, pgs$page_header_offset[2])$data)
    Output
       [1] 02 00 00 00 03 e6 03 03 88 46 00
    Code
      rle_decode_int(data[-(1:7)], bit_width = as.integer(data[7]), sum(!is.na(d$f)))
    Output
      [1] 0 1 2 3 4

---

    Code
      read_parquet_page(tmp, pgs$page_header_offset[1])$data
    Output
       [1] 01 00 00 00 41 01 00 00 00 42 01 00 00 00 43 01 00 00 00 44 01 00 00 00 45
    Code
      read_parquet_page(tmp, pgs$page_header_offset[2])$data
    Output
       [1] 02 00 00 00 03 e6 03 03 88 46 00

# Factor levels not in the data

    Code
      read_parquet_page(tmp, pgs$page_header_offset[1])$data
    Output
        [1] 01 00 00 00 61 01 00 00 00 62 01 00 00 00 63 01 00 00 00 64 01 00 00 00 65
       [26] 01 00 00 00 66 01 00 00 00 67 01 00 00 00 68 01 00 00 00 69 01 00 00 00 6a
       [51] 01 00 00 00 6b 01 00 00 00 6c 01 00 00 00 6d 01 00 00 00 6e 01 00 00 00 6f
       [76] 01 00 00 00 70 01 00 00 00 71 01 00 00 00 72 01 00 00 00 73 01 00 00 00 74
      [101] 01 00 00 00 75 01 00 00 00 76 01 00 00 00 77 01 00 00 00 78 01 00 00 00 79
      [126] 01 00 00 00 7a

# write difftime

    Code
      as.data.frame(d2)
    Output
               h
      1 600 secs

