test_that("parse_encoding", {
  expect_snapshot({
    names(mtcars)
    parse_encoding(NULL, mtcars)
    parse_encoding("PLAIN", mtcars)
    parse_encoding(c(disp = "RLE_DICTIONARY"), mtcars)
    parse_encoding(c(disp = "RLE_DICTIONARY", vs = "PLAIN"), mtcars)
    parse_encoding(c(disp = "RLE", "PLAIN"), mtcars)
    parse_encoding(c(disp = "RLE", "PLAIN", vs = "PLAIN"), mtcars)
  })

  expect_snapshot(error = TRUE, {
    parse_encoding(1:2, mtcars)
    parse_encoding(c("PLAIN", "foobar"), mtcars)
    parse_encoding(c(foo = "PLAIN", foo = "RLE"), mtcars)
    parse_encoding(c(disp = "PLAIN", foo = "RLE"), mtcars)
  })
})
