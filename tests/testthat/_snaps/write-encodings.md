# parse_encoding

    Code
      names(mtcars)
    Output
       [1] "mpg"  "cyl"  "disp" "hp"   "drat" "wt"   "qsec" "vs"   "am"   "gear"
      [11] "carb"
    Code
      parse_encoding(NULL, mtcars)
    Output
       mpg  cyl disp   hp drat   wt qsec   vs   am gear carb 
        NA   NA   NA   NA   NA   NA   NA   NA   NA   NA   NA 
    Code
      parse_encoding("PLAIN", mtcars)
    Output
          mpg     cyl    disp      hp    drat      wt    qsec      vs      am    gear 
      "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" 
         carb 
      "PLAIN" 
    Code
      parse_encoding(c(disp = "RLE_DICTIONARY"), mtcars)
    Output
                   mpg              cyl             disp               hp 
                    NA               NA "RLE_DICTIONARY"               NA 
                  drat               wt             qsec               vs 
                    NA               NA               NA               NA 
                    am             gear             carb 
                    NA               NA               NA 
    Code
      parse_encoding(c(disp = "RLE_DICTIONARY", vs = "PLAIN"), mtcars)
    Output
                   mpg              cyl             disp               hp 
                    NA               NA "RLE_DICTIONARY"               NA 
                  drat               wt             qsec               vs 
                    NA               NA               NA          "PLAIN" 
                    am             gear             carb 
                    NA               NA               NA 
    Code
      parse_encoding(c(disp = "RLE", "PLAIN"), mtcars)
    Output
          mpg     cyl    disp      hp    drat      wt    qsec      vs      am    gear 
      "PLAIN" "PLAIN"   "RLE" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" 
         carb 
      "PLAIN" 
    Code
      parse_encoding(c(disp = "RLE", "PLAIN", vs = "PLAIN"), mtcars)
    Output
          mpg     cyl    disp      hp    drat      wt    qsec      vs      am    gear 
      "PLAIN" "PLAIN"   "RLE" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" 
         carb 
      "PLAIN" 

---

    Code
      parse_encoding(1:2, mtcars)
    Condition
      Error in `parse_encoding()`:
      ! `encoding` must be `NULL` or a character vector
    Code
      parse_encoding(c("PLAIN", "foobar"), mtcars)
    Condition
      Error in `parse_encoding()`:
      ! `encoding` contains at least one unknown encoding
    Code
      parse_encoding(c(foo = "PLAIN", foo = "RLE"), mtcars)
    Condition
      Error in `parse_encoding()`:
      ! names of `encoding` must be unique
    Code
      parse_encoding(c(disp = "PLAIN", foo = "RLE"), mtcars)
    Condition
      Error in `parse_encoding()`:
      ! names of `encoding` must match names of `x`

