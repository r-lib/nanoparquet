# read factors, marked by Arrow

    Code
      res[1:5, ]
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
      res2 <- apply_arrow_schema(res, pf)
    Condition
      Warning in `value[[3L]]()`:
      Failed to parse Arrow schema from parquet file at 'data/factor.parquet'

