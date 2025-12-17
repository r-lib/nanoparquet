# RLE encode integers

RLE encode integers

## Usage

``` r
rle_encode_int(x)
```

## Arguments

- x:

  Integer vector.

## Value

Raw vector, the encoded integers. It has two attributes:

- `bit_length`: the number of bits needed to encode the input, and

- `length`: length of the original integer input.

## See also

[`rle_decode_int()`](https://nanoparquet.r-lib.org/dev/reference/rle_decode_int.md)

Other encodings:
[`rle_decode_int()`](https://nanoparquet.r-lib.org/dev/reference/rle_decode_int.md)
