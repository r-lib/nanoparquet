# RLE decode integers

RLE decode integers

## Usage

``` r
rle_decode_int(
  x,
  bit_width = attr(x, "bit_width"),
  length = attr(x, "length") %||% NA
)
```

## Arguments

- x:

  Raw vector of the encoded integers.

- bit_width:

  Bit width used for the encoding.

- length:

  Length of the output. If `NA` then we assume that `x` starts with
  length of the output, encoded as a 4 byte integer.

## Value

The decoded integer vector.

## See also

[`rle_encode_int()`](https://nanoparquet.r-lib.org/reference/rle_encode_int.md)

Other encodings:
[`rle_encode_int()`](https://nanoparquet.r-lib.org/reference/rle_encode_int.md)
