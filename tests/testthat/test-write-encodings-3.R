test_that("RLE BP encoder", {
  cases <- list(
    # bw 1
    c(rep(0L, 8), rep(1L, 8)),
    c(rep(0L, 9), rep(1L, 9)),
    c(rep(0L, 7), rep(1L, 7)),
    # bw 2
    c(rep(1L, 6), rep(2L, 6)),
    c(rep(1L, 7), rep(2L, 7)),
    c(rep(1L, 5), rep(2L, 5)),
    # bw 3
    c(rep(1L, 4), rep(7L, 4)),
    c(rep(1L, 5), rep(7L, 5)),
    c(rep(1L, 3), rep(7L, 3)),
    # bw 4
    c(rep(1L, 4), rep(15L, 4)),
    c(rep(1L, 5), rep(15L, 5)),
    c(rep(1L, 3), rep(15L, 3)),
    # bw 5
    c(rep(1L, 3), rep(31L, 3)),
    c(rep(1L, 4), rep(31L, 4)),
    c(rep(1L, 2), rep(31L, 2)),
    # bw 6
    c(rep(1L, 3), rep(63L, 3)),
    c(rep(1L, 4), rep(63L, 4)),
    c(rep(1L, 2), rep(63L, 2)),
    # bw 7
    c(rep(1L, 3), rep(127L, 3)),
    c(rep(1L, 4), rep(127L, 4)),
    c(rep(1L, 2), rep(127L, 2)),
    # bw 8
    c(rep(1L, 3), rep(255L, 3)),
    c(rep(1L, 4), rep(255L, 4)),
    c(rep(1L, 2), rep(255L, 2)),
    # bw 9
    c(rep(1L, 3), rep(511L, 3)),
    c(rep(1L, 4), rep(511L, 4)),
    c(rep(1L, 2), rep(511L, 2)),
    # bw 10
    c(rep(1L, 3), rep(1023L, 3)),
    c(rep(1L, 4), rep(1023L, 4)),
    c(rep(1L, 2), rep(1023L, 2)),
    # bw 11
    c(rep(1L, 3), rep(2047L, 3)),
    c(rep(1L, 4), rep(2047L, 4)),
    c(rep(1L, 2), rep(2047L, 2))
  )

  for (case in cases) {
    expect_equal(rle_decode_int(rle_encode_int(case)), case)
    case <- rep(case, 100)
    expect_equal(rle_decode_int(rle_encode_int(case)), case)
  }
})
