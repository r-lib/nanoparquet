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
      as.data.frame(read_parquet(pf))
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
      as.data.frame(read_parquet(pf))
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
      as.data.frame(read_parquet(pf))
    Output
        FirstName        Data
      1      John Hello World
      2      John Hello World
      3      John Hello World
      4      John Hello World
      5      John Hello World

# Tricky V2 data page

    Code
      as.data.frame(read_parquet(pf))
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

# DELTA_BIT_PACKED encoding

    Code
      stat
    Output
      $status
      [1] 0
      
      $stdout
      [1] ""
      
      $stderr
      NULL
      
      $timeout
      [1] FALSE
      

---

    Code
      stat
    Output
      $status
      [1] 0
      
      $stdout
      [1] ""
      
      $stderr
      NULL
      
      $timeout
      [1] FALSE
      

---

    Code
      stat
    Output
      $status
      [1] 0
      
      $stdout
      [1] ""
      
      $stderr
      NULL
      
      $timeout
      [1] FALSE
      

---

    Code
      stat
    Output
      $status
      [1] 0
      
      $stdout
      [1] ""
      
      $stderr
      NULL
      
      $timeout
      [1] FALSE
      

---

    Code
      stat
    Output
      $status
      [1] 0
      
      $stdout
      [1] ""
      
      $stderr
      NULL
      
      $timeout
      [1] FALSE
      

