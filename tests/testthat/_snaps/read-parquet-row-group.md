# plain and simple

    Code
      as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1]
    Output
        id total_byte_size num_rows file_offset total_compressed_size ordinal
      1  0             889       10          NA                    NA      NA
      2  1             895       10          NA                    NA      NA
      3  2             924       10          NA                    NA      NA
      4  3             447        2          NA                    NA      NA

---

    Code
      as.data.frame(read_parquet_row_group(tmp, 0L))
    Output
                       nam  mpg cyl  disp  hp drat    wt  qsec vs am gear carb large
      1          Mazda RX4 21.0   6 160.0 110 3.90 2.620 16.46  0  1    4    4  TRUE
      2      Mazda RX4 Wag 21.0   6 160.0 110 3.90 2.875 17.02  0  1    4    4  TRUE
      3         Datsun 710 22.8   4 108.0  93 3.85 2.320 18.61  1  1    4    1 FALSE
      4     Hornet 4 Drive 21.4   6 258.0 110 3.08 3.215 19.44  1  0    3    1  TRUE
      5  Hornet Sportabout 18.7   8 360.0 175 3.15 3.440 17.02  0  0    3    2  TRUE
      6            Valiant 18.1   6 225.0 105 2.76 3.460 20.22  1  0    3    1  TRUE
      7         Duster 360 14.3   8 360.0 245 3.21 3.570 15.84  0  0    3    4  TRUE
      8          Merc 240D 24.4   4 146.7  62 3.69 3.190 20.00  1  0    4    2 FALSE
      9           Merc 230 22.8   4 140.8  95 3.92 3.150 22.90  1  0    4    2 FALSE
      10          Merc 280 19.2   6 167.6 123 3.92 3.440 18.30  1  0    4    4  TRUE
    Code
      as.data.frame(read_parquet_row_group(tmp, 1L))
    Output
                         nam  mpg cyl  disp  hp drat    wt  qsec vs am gear carb
      1            Merc 280C 17.8   6 167.6 123 3.92 3.440 18.90  1  0    4    4
      2           Merc 450SE 16.4   8 275.8 180 3.07 4.070 17.40  0  0    3    3
      3           Merc 450SL 17.3   8 275.8 180 3.07 3.730 17.60  0  0    3    3
      4          Merc 450SLC 15.2   8 275.8 180 3.07 3.780 18.00  0  0    3    3
      5   Cadillac Fleetwood 10.4   8 472.0 205 2.93 5.250 17.98  0  0    3    4
      6  Lincoln Continental 10.4   8 460.0 215 3.00 5.424 17.82  0  0    3    4
      7    Chrysler Imperial 14.7   8 440.0 230 3.23 5.345 17.42  0  0    3    4
      8             Fiat 128 32.4   4  78.7  66 4.08 2.200 19.47  1  1    4    1
      9          Honda Civic 30.4   4  75.7  52 4.93 1.615 18.52  1  1    4    2
      10      Toyota Corolla 33.9   4  71.1  65 4.22 1.835 19.90  1  1    4    1
         large
      1   TRUE
      2   TRUE
      3   TRUE
      4   TRUE
      5   TRUE
      6   TRUE
      7   TRUE
      8  FALSE
      9  FALSE
      10 FALSE
    Code
      as.data.frame(read_parquet_row_group(tmp, 2L))
    Output
                      nam  mpg cyl  disp  hp drat    wt  qsec vs am gear carb large
      1     Toyota Corona 21.5   4 120.1  97 3.70 2.465 20.01  1  0    3    1 FALSE
      2  Dodge Challenger 15.5   8 318.0 150 2.76 3.520 16.87  0  0    3    2  TRUE
      3       AMC Javelin 15.2   8 304.0 150 3.15 3.435 17.30  0  0    3    2  TRUE
      4        Camaro Z28 13.3   8 350.0 245 3.73 3.840 15.41  0  0    3    4  TRUE
      5  Pontiac Firebird 19.2   8 400.0 175 3.08 3.845 17.05  0  0    3    2  TRUE
      6         Fiat X1-9 27.3   4  79.0  66 4.08 1.935 18.90  1  1    4    1 FALSE
      7     Porsche 914-2 26.0   4 120.3  91 4.43 2.140 16.70  0  1    5    2 FALSE
      8      Lotus Europa 30.4   4  95.1 113 3.77 1.513 16.90  1  1    5    2 FALSE
      9    Ford Pantera L 15.8   8 351.0 264 4.22 3.170 14.50  0  1    5    4  TRUE
      10     Ferrari Dino 19.7   6 145.0 175 3.62 2.770 15.50  0  1    5    6  TRUE
    Code
      as.data.frame(read_parquet_row_group(tmp, 3L))
    Output
                  nam  mpg cyl disp  hp drat   wt qsec vs am gear carb large
      1 Maserati Bora 15.0   8  301 335 3.54 3.57 14.6  0  1    5    8  TRUE
      2    Volvo 142E 21.4   4  121 109 4.11 2.78 18.6  1  1    4    2 FALSE

# dicts

    Code
      as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1]
    Output
        id total_byte_size num_rows file_offset total_compressed_size ordinal
      1  0            1072       10          NA                    NA      NA
      2  1            1111       10          NA                    NA      NA
      3  2            1183       10          NA                    NA      NA
      4  3             659        2          NA                    NA      NA

---

    Code
      as.data.frame(read_parquet_row_group(tmp, 0L))
    Output
                       nam  mpg cyl  disp  hp drat    wt  qsec vs am gear carb large
      1          Mazda RX4 21.0   6 160.0 110 3.90 2.620 16.46  0  1    4    4  TRUE
      2      Mazda RX4 Wag 21.0   6 160.0 110 3.90 2.875 17.02  0  1    4    4  TRUE
      3         Datsun 710 22.8   4 108.0  93 3.85 2.320 18.61  1  1    4    1 FALSE
      4     Hornet 4 Drive 21.4   6 258.0 110 3.08 3.215 19.44  1  0    3    1  TRUE
      5  Hornet Sportabout 18.7   8 360.0 175 3.15 3.440 17.02  0  0    3    2  TRUE
      6            Valiant 18.1   6 225.0 105 2.76 3.460 20.22  1  0    3    1  TRUE
      7         Duster 360 14.3   8 360.0 245 3.21 3.570 15.84  0  0    3    4  TRUE
      8          Merc 240D 24.4   4 146.7  62 3.69 3.190 20.00  1  0    4    2 FALSE
      9           Merc 230 22.8   4 140.8  95 3.92 3.150 22.90  1  0    4    2 FALSE
      10          Merc 280 19.2   6 167.6 123 3.92 3.440 18.30  1  0    4    4  TRUE
    Code
      as.data.frame(read_parquet_row_group(tmp, 1L))
    Output
                         nam  mpg cyl  disp  hp drat    wt  qsec vs am gear carb
      1            Merc 280C 17.8   6 167.6 123 3.92 3.440 18.90  1  0    4    4
      2           Merc 450SE 16.4   8 275.8 180 3.07 4.070 17.40  0  0    3    3
      3           Merc 450SL 17.3   8 275.8 180 3.07 3.730 17.60  0  0    3    3
      4          Merc 450SLC 15.2   8 275.8 180 3.07 3.780 18.00  0  0    3    3
      5   Cadillac Fleetwood 10.4   8 472.0 205 2.93 5.250 17.98  0  0    3    4
      6  Lincoln Continental 10.4   8 460.0 215 3.00 5.424 17.82  0  0    3    4
      7    Chrysler Imperial 14.7   8 440.0 230 3.23 5.345 17.42  0  0    3    4
      8             Fiat 128 32.4   4  78.7  66 4.08 2.200 19.47  1  1    4    1
      9          Honda Civic 30.4   4  75.7  52 4.93 1.615 18.52  1  1    4    2
      10      Toyota Corolla 33.9   4  71.1  65 4.22 1.835 19.90  1  1    4    1
         large
      1   TRUE
      2   TRUE
      3   TRUE
      4   TRUE
      5   TRUE
      6   TRUE
      7   TRUE
      8  FALSE
      9  FALSE
      10 FALSE
    Code
      as.data.frame(read_parquet_row_group(tmp, 2L))
    Output
                      nam  mpg cyl  disp  hp drat    wt  qsec vs am gear carb large
      1     Toyota Corona 21.5   4 120.1  97 3.70 2.465 20.01  1  0    3    1 FALSE
      2  Dodge Challenger 15.5   8 318.0 150 2.76 3.520 16.87  0  0    3    2  TRUE
      3       AMC Javelin 15.2   8 304.0 150 3.15 3.435 17.30  0  0    3    2  TRUE
      4        Camaro Z28 13.3   8 350.0 245 3.73 3.840 15.41  0  0    3    4  TRUE
      5  Pontiac Firebird 19.2   8 400.0 175 3.08 3.845 17.05  0  0    3    2  TRUE
      6         Fiat X1-9 27.3   4  79.0  66 4.08 1.935 18.90  1  1    4    1 FALSE
      7     Porsche 914-2 26.0   4 120.3  91 4.43 2.140 16.70  0  1    5    2 FALSE
      8      Lotus Europa 30.4   4  95.1 113 3.77 1.513 16.90  1  1    5    2 FALSE
      9    Ford Pantera L 15.8   8 351.0 264 4.22 3.170 14.50  0  1    5    4  TRUE
      10     Ferrari Dino 19.7   6 145.0 175 3.62 2.770 15.50  0  1    5    6  TRUE
    Code
      as.data.frame(read_parquet_row_group(tmp, 3L))
    Output
                  nam  mpg cyl disp  hp drat   wt qsec vs am gear carb large
      1 Maserati Bora 15.0   8  301 335 3.54 3.57 14.6  0  1    5    8  TRUE
      2    Volvo 142E 21.4   4  121 109 4.11 2.78 18.6  1  1    4    2 FALSE

