# null_count is written

    Code
      as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]][, c("row_group",
        "column", "null_count")])
    Output
         row_group column null_count
      1          0      0          1
      2          0      1          1
      3          0      2          1
      4          0      3          1
      5          0      4          1
      6          0      5          1
      7          0      6          1
      8          0      7          1
      9          0      8          1
      10         0      9          1
      11         0     10          0
      12         0     11          0
      13         0     12          0
      14         1      0          0
      15         1      1          0
      16         1      2          0
      17         1      3          0
      18         1      4          0
      19         1      5          0
      20         1      6          0
      21         1      7          0
      22         1      8          0
      23         1      9          0
      24         1     10          1
      25         1     11          1
      26         1     12          1
      27         2      0          0
      28         2      1          0
      29         2      2          0
      30         2      3          0
      31         2      4          0
      32         2      5          0
      33         2      6          0
      34         2      7          0
      35         2      8          0
      36         2      9          0
      37         2     10          0
      38         2     11          0
      39         2     12          0
      40         3      0          0
      41         3      1          0
      42         3      2          0
      43         3      3          0
      44         3      4          0
      45         3      5          0
      46         3      6          0
      47         3      7          0
      48         3      8          0
      49         3      9          0
      50         3     10          0
      51         3     11          0
      52         3     12          0

# min/max for integers

    Code
      mtd[mtd$column == 2, c("row_group", "column", "min_value", "max_value")]
    Output
         row_group column    min_value    max_value
      3          0      2 04, 00, .... 04, 00, ....
      16         1      2 04, 00, .... 04, 00, ....
      29         2      2 06, 00, .... 06, 00, ....
      42         3      2 06, 00, .... 08, 00, ....
      55         4      2 08, 00, .... 08, 00, ....
      68         5      2 08, 00, .... 08, 00, ....
      81         6      2 08, 00, .... 08, 00, ....

---

    Code
      mtd[mtd$column == 2, c("row_group", "column", "min_value", "max_value")]
    Output
         row_group column    min_value    max_value
      3          0      2 04, 00, .... 04, 00, ....
      16         1      2 04, 00, .... 04, 00, ....
      29         2      2 06, 00, .... 06, 00, ....
      42         3      2 06, 00, .... 08, 00, ....
      55         4      2 08, 00, .... 08, 00, ....
      68         5      2 08, 00, .... 08, 00, ....
      81         6      2 08, 00, .... 08, 00, ....

