# Benchmarks

## Goals

First, I want to measure nanoparquet’s speed relative to good quality
CSV readers and writers, and also look at the sizes of the Parquet and
CSV files.

Second, I want to see how nanoparquet fares relative to other Parquet
implementations available from R.

``` r
library(dplyr)
library(gt)
library(gtExtras)
```

## Data sets

I used use three data sets: small, medium and large. The small data set
is the
[`nycflights13::flights`](https://rdrr.io/pkg/nycflights13/man/flights.html)
data set, as is. The medium data set contains 20 copies of the small
data set. The large data set contains 200 copies of the small data set.
See the `gen_data()` function in the [`benchmark-funcs.R`
file](https://github.com/r-lib/nanoparquet/blob/main/vignettes/articles/benchmarks-funcs.R)
in the nanoparquet GitHub repository.

Some basic information about each data set:

Show the code

``` r
if (file.exists(file.path(me, "data-info.parquet"))) {
  info_tab <- nanoparquet::read_parquet(file.path(me, "data-info.parquet"))
} else {
  get_data_info <- function(x) {
    list(dim = dim(x), size = object.size(x))
  }
  info <- lapply(data_sizes, function(s) get_data_info(gen_data(s)))
  info_tab <- data.frame(
    check.names = FALSE,
    name = data_sizes,
    rows = sapply(info, "[[", "dim")[1,],
    columns = sapply(info, "[[", "dim")[2,],
    "size in memory" = sapply(info, "[[", "size")
  )
  nanoparquet::write_parquet(info_tab, file.path(me, "data-info.parquet"))
}
info_tab |>
  gt() |>
  tab_header(title = "Data sets") |>
  tab_options(table.align = "left") |>
  fmt_integer() |>
  fmt_bytes(columns = "size in memory")
```

| Data sets |            |         |                |
|-----------|------------|---------|----------------|
| name      | rows       | columns | size in memory |
| small     | 336,776    | 19      | 40.7 MB        |
| medium    | 6,735,520  | 19      | 808.5 MB       |
| large     | 67,355,200 | 19      | 8.1 GB         |

A quick look at the data:

Show the code

``` r
head(nycflights13::flights)
```

    #> # A tibble: 6 × 19
    #>    year month   day dep_time sched_dep_time dep_delay arr_time sched_arr_time
    #>   <int> <int> <int>    <int>          <int>     <dbl>    <int>          <int>
    #> 1  2013     1     1      517            515         2      830            819
    #> 2  2013     1     1      533            529         4      850            830
    #> 3  2013     1     1      542            540         2      923            850
    #> 4  2013     1     1      544            545        -1     1004           1022
    #> 5  2013     1     1      554            600        -6      812            837
    #> 6  2013     1     1      554            558        -4      740            728
    #> # ℹ 11 more variables: arr_delay <dbl>, carrier <chr>, flight <int>,
    #> #   tailnum <chr>, origin <chr>, dest <chr>, air_time <dbl>, distance <dbl>,
    #> #   hour <dbl>, minute <dbl>, time_hour <dttm>

Show the code

``` r
dplyr::glimpse(nycflights13::flights)
```

    #> Rows: 336,776
    #> Columns: 19
    #> $ year           <int> 2013, 2013, 2013, 2013, 2013, 2013, 2013, 2013, 2013, 2…
    #> $ month          <int> 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1…
    #> $ day            <int> 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1…
    #> $ dep_time       <int> 517, 533, 542, 544, 554, 554, 555, 557, 557, 558, 558, …
    #> $ sched_dep_time <int> 515, 529, 540, 545, 600, 558, 600, 600, 600, 600, 600, …
    #> $ dep_delay      <dbl> 2, 4, 2, -1, -6, -4, -5, -3, -3, -2, -2, -2, -2, -2, -1…
    #> $ arr_time       <int> 830, 850, 923, 1004, 812, 740, 913, 709, 838, 753, 849,…
    #> $ sched_arr_time <int> 819, 830, 850, 1022, 837, 728, 854, 723, 846, 745, 851,…
    #> $ arr_delay      <dbl> 11, 20, 33, -18, -25, 12, 19, -14, -8, 8, -2, -3, 7, -1…
    #> $ carrier        <chr> "UA", "UA", "AA", "B6", "DL", "UA", "B6", "EV", "B6", "…
    #> $ flight         <int> 1545, 1714, 1141, 725, 461, 1696, 507, 5708, 79, 301, 4…
    #> $ tailnum        <chr> "N14228", "N24211", "N619AA", "N804JB", "N668DN", "N394…
    #> $ origin         <chr> "EWR", "LGA", "JFK", "JFK", "LGA", "EWR", "EWR", "LGA",…
    #> $ dest           <chr> "IAH", "IAH", "MIA", "BQN", "ATL", "ORD", "FLL", "IAD",…
    #> $ air_time       <dbl> 227, 227, 160, 183, 116, 150, 158, 53, 140, 138, 149, 1…
    #> $ distance       <dbl> 1400, 1416, 1089, 1576, 762, 719, 1065, 229, 944, 733, …
    #> $ hour           <dbl> 5, 5, 5, 5, 6, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 5, 6, 6, 6…
    #> $ minute         <dbl> 15, 29, 40, 45, 0, 58, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 0…
    #> $ time_hour      <dttm> 2013-01-01 05:00:00, 2013-01-01 05:00:00, 2013-01-01 0…

## Parquet implementations

I ran nanoparquet, Arrow and DuckDB. I also ran data.table without and
with compression and readr, to read/write CSV files. I used the running
time of readr as the baseline.

I ran each benchmark three times and record the results of the third
run. This is to make sure that the data and the software is not swapped
out by the OS. (Except for readr on the large data set, because it would
take too long.)

Show the code

``` r
if (file.exists(file.path(me, "results.parquet"))) {
  results <- nanoparquet::read_parquet(file.path(me, "results.parquet"))
} else {
  results <- NULL
  lapply(data_sizes, function(s) {
    lapply(variants, function(v) {
      r <- if (v == "readr" && s == "large") {
        measure(v, s)
      } else {
        measure(v, s)
        measure(v, s)
        measure(v, s)
      }
      results <<- rbind(results, r)
    })
  })
  nanoparquet::write_parquet(results, file.path(me, "results.parquet"))
}
```

I include the complete raw results at the end of this article.

## Parquet vs CSV

The results, focusing on the CSV readers and nanoparquet:

Show the code

``` r
csv_tab_read <- results |>
  filter(software %in% c("nanoparquet", "data.table", "data.table.gz", "readr")) |>
  filter(direction == "read") |>
  mutate(software = case_when(
    software ==  "data.table.gz" ~ "data.table (compressed)",
    .default = software
  )) |>
  rename(`data size` = data_size, time = time_elapsed) |>
  mutate(memory = mem_max_after - mem_before) |>
  mutate(base = tail(time, 1), .by = `data size`) |>
  mutate(speedup = base / time, x = round(speedup, digits = 1)) |>
  select(`data size`, software, time, x, speedup, memory) |>
  mutate(rawtime = time, time = prettyunits::pretty_sec(time)) |>
  rename(`speedup from CSV` = speedup)

csv_tab_read |>
  gt() |>
  tab_header(title = "Parquet vs CSV, reading") |>
  tab_options(table.align = "left") |>
  tab_row_group(md("**small data**"), rows = `data size` == "small", "s") |>
  tab_row_group(md("**medium data**"), rows = `data size` == "medium", "m") |>
  tab_row_group(md("**large data**"), rows = `data size` == "large", "l") |>
  row_group_order(c("s", "m", "l")) |>
  cols_hide(columns = c(`data size`, rawtime)) |>
  cols_align(columns = time, align = "right") |>
  fmt_bytes(columns = memory) |>
  gt_plt_bar(column = `speedup from CSV`, color = "steelblue") |>
  opt_css("td svg rect:first-child { fill: transparent !important; }")
```

| Parquet vs CSV, reading |       |      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |
|-------------------------|-------|------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| software                | time  | x    | speedup from CSV                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | memory   |
| **small data**          |       |      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |
| nanoparquet             | 21ms  | 15.0 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9Ijk4LjM3IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 73.6 MB  |
| data.table              | 39ms  | 7.7  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjUwLjQ1IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 67.2 MB  |
| data.table (compressed) | 107ms | 2.8  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjE4LjU2IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 127.1 MB |
| readr                   | 300ms | 1.0  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjYuNTYiIGhlaWdodD0iMTIuNDAiIHN0eWxlPSJzdHJva2Utd2lkdGg6IDEuMDc7IHN0cm9rZTogbm9uZTsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7IHN0cm9rZS1saW5lam9pbjogbWl0ZXI7IGZpbGw6ICM0NjgyQjQ7IiAvPjxsaW5lIHgxPSI1LjAyIiB5MT0iMTQuMTciIHgyPSI1LjAyIiB5Mj0iMC4wMDAwMDAwMDAwMDAwMDE4IiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2UtbGluZWNhcDogYnV0dDsiPjwvbGluZT48L2c+PC9nPjwvc3ZnPg==) | 199.1 MB |
| **medium data**         |       |      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |
| nanoparquet             | 721ms | 5.1  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjMzLjM1IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 1.7 GB   |
| data.table              | 582ms | 6.3  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjQxLjM4IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 1.3 GB   |
| data.table (compressed) | 1.5s  | 2.4  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjE1Ljk0IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 1.4 GB   |
| readr                   | 3.7s  | 1.0  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjYuNTYiIGhlaWdodD0iMTIuNDAiIHN0eWxlPSJzdHJva2Utd2lkdGg6IDEuMDc7IHN0cm9rZTogbm9uZTsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7IHN0cm9rZS1saW5lam9pbjogbWl0ZXI7IGZpbGw6ICM0NjgyQjQ7IiAvPjxsaW5lIHgxPSI1LjAyIiB5MT0iMTQuMTciIHgyPSI1LjAyIiB5Mj0iMC4wMDAwMDAwMDAwMDAwMDE4IiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2UtbGluZWNhcDogYnV0dDsiPjwvbGluZT48L2c+PC9nPjwvc3ZnPg==) | 3 GB     |
| **large data**          |       |      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |
| nanoparquet             | 5.9s  | 5.5  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjM1LjgwIiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 14.1 GB  |
| data.table              | 4s    | 8.0  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjUyLjI4IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 13 GB    |
| data.table (compressed) | 11.4s | 2.8  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjE4LjQyIiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 13.1 GB  |
| readr                   | 32.2s | 1.0  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjYuNTYiIGhlaWdodD0iMTIuNDAiIHN0eWxlPSJzdHJva2Utd2lkdGg6IDEuMDc7IHN0cm9rZTogbm9uZTsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7IHN0cm9rZS1saW5lam9pbjogbWl0ZXI7IGZpbGw6ICM0NjgyQjQ7IiAvPjxsaW5lIHgxPSI1LjAyIiB5MT0iMTQuMTciIHgyPSI1LjAyIiB5Mj0iMC4wMDAwMDAwMDAwMDAwMDE4IiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2UtbGluZWNhcDogYnV0dDsiPjwvbGluZT48L2c+PC9nPjwvc3ZnPg==) | 21.5 GB  |

Notes:

- The single-threaded nanoparquet Parquet-reader is competitive. It can
  read a compressed Parquet file just as fast as the state of the art
  uncompressed CSV reader that uses at least 2 threads.

The nanoparquet vs CSV results when writing Parquet or CSV files:

Show the code

``` r
csv_tab_write <- results |>
  filter(software %in% c("nanoparquet", "data.table", "data.table.gz", "readr")) |>
  filter(direction == "write") |>
  mutate(software = case_when(
    software ==  "data.table.gz" ~ "data.table (compressed)",
    .default = software
  )) |>
  rename(`data size` = data_size, time = time_elapsed, `file size` = file_size) |>
  mutate(memory = mem_max_after - mem_before) |>
  mutate(base = tail(time, 1), .by = `data size`) |>
  mutate(speedup = base / time, x = round(speedup, digits = 1)) |>
  select(`data size`, software, time, x, speedup, memory, `file size`) |>
  mutate(rawtime = time, time = prettyunits::pretty_sec(time)) |>
  rename(`speedup from CSV` = speedup)

csv_tab_write |>
  gt() |>
  tab_header(title = "Parquet vs CSV, writing") |>
  tab_options(table.align = "left") |>
  tab_row_group(md("**small data**"), rows = `data size` == "small", "s") |>
  tab_row_group(md("**medium data**"), rows = `data size` == "medium", "m") |>
  tab_row_group(md("**large data**"), rows = `data size` == "large", "l") |>
  row_group_order(c("s", "m", "l")) |>
  cols_hide(columns = c(`data size`, rawtime)) |>
  cols_align(columns = time, align = "right") |>
  fmt_bytes(columns = c(memory, `file size`)) |>
  gt_plt_bar(column = `speedup from CSV`, color = "steelblue") |>
  opt_css("td svg rect:first-child { fill: transparent !important; }")
```

| Parquet vs CSV, writing |          |      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |           |
|-------------------------|----------|------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-----------|
| software                | time     | x    | speedup from CSV                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | memory   | file size |
| **small data**          |          |      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |           |
| nanoparquet             | 84ms     | 6.3  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjE5LjQ1IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 67.4 MB  | 5.7 MB    |
| data.table              | 35ms     | 15.4 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjQ4LjA1IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 12.9 MB  | 31 MB     |
| data.table (compressed) | 178ms    | 3.0  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjkuMjMiIGhlaWdodD0iMTIuNDAiIHN0eWxlPSJzdHJva2Utd2lkdGg6IDEuMDc7IHN0cm9rZTogbm9uZTsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7IHN0cm9rZS1saW5lam9pbjogbWl0ZXI7IGZpbGw6ICM0NjgyQjQ7IiAvPjxsaW5lIHgxPSI1LjAyIiB5MT0iMTQuMTciIHgyPSI1LjAyIiB5Mj0iMC4wMDAwMDAwMDAwMDAwMDE4IiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2UtbGluZWNhcDogYnV0dDsiPjwvbGluZT48L2c+PC9nPjwvc3ZnPg==) | 15.7 MB  | 8.3 MB    |
| readr                   | 526ms    | 1.0  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjMuMTEiIGhlaWdodD0iMTIuNDAiIHN0eWxlPSJzdHJva2Utd2lkdGg6IDEuMDc7IHN0cm9rZTogbm9uZTsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7IHN0cm9rZS1saW5lam9pbjogbWl0ZXI7IGZpbGw6ICM0NjgyQjQ7IiAvPjxsaW5lIHgxPSI1LjAyIiB5MT0iMTQuMTciIHgyPSI1LjAyIiB5Mj0iMC4wMDAwMDAwMDAwMDAwMDE4IiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2UtbGluZWNhcDogYnV0dDsiPjwvbGluZT48L2c+PC9nPjwvc3ZnPg==) | 52.9 MB  | 31.1 MB   |
| **medium data**         |          |      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |           |
| nanoparquet             | 1.7s     | 5.8  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjE4LjA2IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 676.2 MB | 111.4 MB  |
| data.table              | 318ms    | 30.2 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjkzLjg1IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 11.4 MB  | 619.2 MB  |
| data.table (compressed) | 3.2s     | 3.0  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjkuMjMiIGhlaWdodD0iMTIuNDAiIHN0eWxlPSJzdHJva2Utd2lkdGg6IDEuMDc7IHN0cm9rZTogbm9uZTsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7IHN0cm9rZS1saW5lam9pbjogbWl0ZXI7IGZpbGw6ICM0NjgyQjQ7IiAvPjxsaW5lIHgxPSI1LjAyIiB5MT0iMTQuMTciIHgyPSI1LjAyIiB5Mj0iMC4wMDAwMDAwMDAwMDAwMDE4IiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2UtbGluZWNhcDogYnV0dDsiPjwvbGluZT48L2c+PC9nPjwvc3ZnPg==) | 18.8 MB  | 165.2 MB  |
| readr                   | 9.6s     | 1.0  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjMuMTEiIGhlaWdodD0iMTIuNDAiIHN0eWxlPSJzdHJva2Utd2lkdGg6IDEuMDc7IHN0cm9rZTogbm9uZTsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7IHN0cm9rZS1saW5lam9pbjogbWl0ZXI7IGZpbGw6ICM0NjgyQjQ7IiAvPjxsaW5lIHgxPSI1LjAyIiB5MT0iMTQuMTciIHgyPSI1LjAyIiB5Mj0iMC4wMDAwMDAwMDAwMDAwMDE4IiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2UtbGluZWNhcDogYnV0dDsiPjwvbGluZT48L2c+PC9nPjwvc3ZnPg==) | 836.1 MB | 621.1 MB  |
| **large data**          |          |      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |           |
| nanoparquet             | 15.6s    | 6.1  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjE4Ljk3IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 4 GB     | 1.1 GB    |
| data.table              | 3s       | 31.6 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9Ijk4LjM3IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 12.5 MB  | 6.2 GB    |
| data.table (compressed) | 30.7s    | 3.1  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjkuNjUiIGhlaWdodD0iMTIuNDAiIHN0eWxlPSJzdHJva2Utd2lkdGg6IDEuMDc7IHN0cm9rZTogbm9uZTsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7IHN0cm9rZS1saW5lam9pbjogbWl0ZXI7IGZpbGw6ICM0NjgyQjQ7IiAvPjxsaW5lIHgxPSI1LjAyIiB5MT0iMTQuMTciIHgyPSI1LjAyIiB5Mj0iMC4wMDAwMDAwMDAwMDAwMDE4IiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2UtbGluZWNhcDogYnV0dDsiPjwvbGluZT48L2c+PC9nPjwvc3ZnPg==) | 23.1 MB  | 1.7 GB    |
| readr                   | 1m 35.4s | 1.0  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjMuMTEiIGhlaWdodD0iMTIuNDAiIHN0eWxlPSJzdHJva2Utd2lkdGg6IDEuMDc7IHN0cm9rZTogbm9uZTsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7IHN0cm9rZS1saW5lam9pbjogbWl0ZXI7IGZpbGw6ICM0NjgyQjQ7IiAvPjxsaW5lIHgxPSI1LjAyIiB5MT0iMTQuMTciIHgyPSI1LjAyIiB5Mj0iMC4wMDAwMDAwMDAwMDAwMDE4IiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2UtbGluZWNhcDogYnV0dDsiPjwvbGluZT48L2c+PC9nPjwvc3ZnPg==) | 8.4 GB   | 6.2 GB    |

Notes:

- The data.table CSV writer is about 6 times as fast as the nanoparquet
  Parquet writer, if the CSV file is uncompressed. The CSV writer uses
  at least 7 threads, the Parquet write is single-threaded.
- The nanoparquet Parquet writer is about 2 times faster than the
  data.table CSV writer if the CSV file is compressed.
- The Parquet files are about 5-6 times smaller than the uncompressed
  CSV files and about 30-35% smaller than the compressed CSV files.

## Parquet implementations

This is the summary of the Parquet readers, for the same three files.

Show the code

``` r
pq_tab_read <- results |>
  filter(software %in% c("nanoparquet", "arrow", "duckdb", "readr")) |>
  filter(direction == "read") |>
  rename(`data size` = data_size, time = time_elapsed) |>
  mutate(memory = mem_max_after - mem_before) |>
  mutate(base = tail(time, 1), .by = `data size`) |>
  mutate(speedup = base / time, x = round(speedup, digits = 1)) |>
  select(`data size`, software, time, x, speedup, memory) |>
  mutate(rawtime = time, time = prettyunits::pretty_sec(time)) |>
  filter(software %in% c("nanoparquet", "arrow", "duckdb")) |>
  mutate(software = case_when(
    software ==  "arrow" ~ "Arrow",
    software == "duckdb" ~ "DuckDB",
    .default = software
  )) |>
  rename(`speedup from CSV` = speedup)

pq_tab_read |>
  gt() |>
  tab_header(title = "Parquet implementations, reading") |>
  tab_options(table.align = "left") |>
  tab_row_group(md("**small data**"), rows = `data size` == "small", "s") |>
  tab_row_group(md("**medium data**"), rows = `data size` == "medium", "m") |>
  tab_row_group(md("**large data**"), rows = `data size` == "large", "l") |>
  row_group_order(c("s", "m", "l")) |>
  cols_hide(columns = c(`data size`, rawtime)) |>
  cols_align(columns = time, align = "right") |>
  fmt_bytes(columns = memory) |>
  gt_plt_bar(column = `speedup from CSV`, color = "steelblue") |>
  opt_css("td svg rect:first-child { fill: transparent !important; }")
```

| Parquet implementations, reading |       |      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |
|----------------------------------|-------|------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| software                         | time  | x    | speedup from CSV                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | memory   |
| **small data**                   |       |      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |
| nanoparquet                      | 21ms  | 15.0 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9Ijk4LjM3IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 73.6 MB  |
| Arrow                            | 26ms  | 12.0 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9Ijc4LjcwIiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 107.2 MB |
| DuckDB                           | 40ms  | 7.7  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjUwLjQ1IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 115.4 MB |
| **medium data**                  |       |      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |
| nanoparquet                      | 721ms | 5.1  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjMzLjM1IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 1.7 GB   |
| Arrow                            | 625ms | 5.9  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjM4LjQ3IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 2 GB     |
| DuckDB                           | 716ms | 5.1  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjMzLjU4IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 2 GB     |
| **large data**                   |       |      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |
| nanoparquet                      | 5.9s  | 5.5  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjM1LjgwIiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 14.1 GB  |
| Arrow                            | 4.8s  | 6.6  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjQzLjQ4IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 17.6 GB  |
| DuckDB                           | 5.3s  | 6.1  | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjM5Ljg3IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 18.8 GB  |

Notes:

- In general, all three implementations perform similarly. nanoparquet
  is very competitive for these three data sets in terms of speed and
  also tends to use the least amount of memory.
- I turned off ALTREP in arrow, so that it reads the data into memory.

The summary for the Parquet writers:

Show the code

``` r
pq_tab_write <- results |>
  filter(software %in% c("nanoparquet", "arrow", "duckdb", "readr")) |>
  filter(direction == "write") |>
  rename(`data size` = data_size, time = time_elapsed, `file size` = file_size) |>
  mutate(memory = mem_max_after - mem_before) |>
  mutate(base = tail(time, 1), .by = `data size`) |>
  mutate(speedup = base / time, x = round(speedup, digits = 1)) |>
  select(`data size`, software, time, x, speedup, memory, `file size`) |>
  mutate(rawtime = time, time = prettyunits::pretty_sec(time)) |>
  filter(software %in% c("nanoparquet", "arrow", "duckdb", "readr")) |>
  mutate(software = case_when(
    software ==  "arrow" ~ "Arrow",
    software == "duckdb" ~ "DuckDB",
    .default = software
  )) |>
  rename(`speedup from CSV` = speedup)

pq_tab_write |>
  gt() |>
  tab_header(title = "Parquet implementations, writing") |>
  tab_options(table.align = "left") |>
  tab_row_group(md("**small data**"), rows = `data size` == "small", "s") |>
  tab_row_group(md("**medium data**"), rows = `data size` == "medium", "m") |>
  tab_row_group(md("**large data**"), rows = `data size` == "large", "l") |>
  row_group_order(c("s", "m", "l")) |>
  cols_hide(columns = c(`data size`, rawtime)) |>
  cols_align(columns = time, align = "right") |>
  fmt_bytes(columns = c(memory, `file size`)) |>
  gt_plt_bar(column = `speedup from CSV`, color = "steelblue") |>
  opt_css("td svg rect:first-child { fill: transparent !important; }")
```

| Parquet implementations, writing |          |     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |           |
|----------------------------------|----------|-----|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-----------|
| software                         | time     | x   | speedup from CSV                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | memory   | file size |
| **small data**                   |          |     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |           |
| nanoparquet                      | 84ms     | 6.3 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjkzLjQwIiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 67.4 MB  | 5.7 MB    |
| Arrow                            | 101ms    | 5.2 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9Ijc4LjQ2IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 25.6 MB  | 5.6 MB    |
| DuckDB                           | 127ms    | 4.2 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjYyLjI3IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 200.7 MB | 5.8 MB    |
| readr                            | 526ms    | 1.0 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjE0Ljk0IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 52.9 MB  | 31.1 MB   |
| **medium data**                  |          |     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |           |
| nanoparquet                      | 1.7s     | 5.8 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9Ijg2LjcxIiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 676.2 MB | 111.4 MB  |
| Arrow                            | 1.9s     | 5.2 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9Ijc2Ljk4IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 281.1 MB | 112.5 MB  |
| DuckDB                           | 1.5s     | 6.6 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9Ijk4LjM3IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 2.7 GB   | 115.9 MB  |
| readr                            | 9.6s     | 1.0 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjE0Ljk0IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 836.1 MB | 621.1 MB  |
| **large data**                   |          |     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |          |           |
| nanoparquet                      | 15.6s    | 6.1 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjkxLjA4IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 4 GB     | 1.1 GB    |
| Arrow                            | 19s      | 5.0 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9Ijc0LjkyIiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 2.7 GB   | 1.1 GB    |
| DuckDB                           | 14.8s    | 6.5 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9Ijk2LjUzIiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 12 GB    | 1.2 GB    |
| readr                            | 1m 35.4s | 1.0 | ![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB3aWR0aD0iMTEzLjM5cHQiIGhlaWdodD0iMTQuMTdwdCIgdmlld2JveD0iMCAwIDExMy4zOSAxNC4xNyI+PGcgY2xhc3M9InN2Z2xpdGUiPjxkZWZzPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBzdHlsZT0ic3Ryb2tlOiBub25lOyBmaWxsOiBub25lOyIgLz48ZGVmcz48Y2xpcHBhdGggaWQ9ImNwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0iPjxyZWN0IHg9IjAuMDAiIHk9IjAuMDAiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIC8+PC9jbGlwcGF0aD48L2RlZnM+PGcgY2xpcC1wYXRoPSJ1cmwoI2NwTUM0d01Id3hNVE11TXpsOE1DNHdNSHd4TkM0eE53PT0pIj48cmVjdCB4PSIwLjAwIiB5PSIwLjAwMDAwMDAwMDAwMDAwMTgiIHdpZHRoPSIxMTMuMzkiIGhlaWdodD0iMTQuMTciIHN0eWxlPSJzdHJva2Utd2lkdGg6IDAuMDA7IHN0cm9rZTogbm9uZTsiIC8+PHJlY3QgeD0iNS4wMiIgeT0iMC44OSIgd2lkdGg9IjE0Ljk0IiBoZWlnaHQ9IjEyLjQwIiBzdHlsZT0ic3Ryb2tlLXdpZHRoOiAxLjA3OyBzdHJva2U6IG5vbmU7IHN0cm9rZS1saW5lY2FwOiBidXR0OyBzdHJva2UtbGluZWpvaW46IG1pdGVyOyBmaWxsOiAjNDY4MkI0OyIgLz48bGluZSB4MT0iNS4wMiIgeTE9IjE0LjE3IiB4Mj0iNS4wMiIgeTI9IjAuMDAwMDAwMDAwMDAwMDAxOCIgc3R5bGU9InN0cm9rZS13aWR0aDogMS4wNzsgc3Ryb2tlLWxpbmVjYXA6IGJ1dHQ7Ij48L2xpbmU+PC9nPjwvZz48L3N2Zz4=) | 8.4 GB   | 6.2 GB    |

Notes:

- nanoparquet is again very competitive in terms of speed, for these
  data sets.

## Conclusions

These results will probably change for a different data sets, or on a
different system. In particular, Arrow and DuckDB are probably faster on
larger systems, where the data is stored on multiple physical disks.

Both Arrow and DuckDB let you run queries on the data without loading it
all into memory first. This is especially important if the data does not
fit into memory at all, not even the columns needed for the analysis.
nanoparquet cannot do this.

However, in general, based on these benchmarks I have good reasons to
trust that the nanoparquet Parquet reader and writer is competitive with
the other implementations available from R, both in terms of speed and
memory use.

If the limitations of nanoparquet are not prohibitive for your use case,
it is a good choice for Parquet I/O.

## Raw benchmark results

These are the raw results. You can scroll to the right if your screen is
not wide enough for the whole table.

Show the code

``` r
suppressPackageStartupMessages(library(pillar))
results <- tibble::as_tibble(results)
print(results, n = Inf)
```

    #> # A tibble: 36 × 10
    #>    software      direction data_size time_user time_system time_elapsed mem_before mem_max_before mem_max_after  file_size
    #>    <chr>         <chr>     <chr>         <dbl>       <dbl>        <dbl>      <dbl>          <dbl>         <dbl>      <dbl>
    #>  1 nanoparquet   read      small        0.0160     0.004         0.0200  165462016      165478400     239026176         NA
    #>  2 nanoparquet   write     small        0.0740     0.009         0.0840  323682304      323993600     391069696    5687753
    #>  3 arrow         read      small        0.0380     0.023         0.0250  166772736      167067648     273956864         NA
    #>  4 arrow         write     small        0.1000     0.0050        0.100   318947328      319258624     344571904    5645892
    #>  5 duckdb        read      small        0.0400     0.00900       0.0390  171130880      171425792     286556160         NA
    #>  6 duckdb        write     small        0.174      0.02          0.126   319307776      319619072     519962624    5807811
    #>  7 data.table    read      small        0.118      0.007         0.0390  167788544      167870464     234995712         NA
    #>  8 data.table    write     small        0.112      0.0050        0.0340  314998784      315293696     327876608   30960660
    #>  9 data.table.gz read      small        0.178      0.015         0.106   163692544      163987456     290832384         NA
    #> 10 data.table.gz write     small        1.10       0.007         0.177   314982400      314982400     330711040    8262799
    #> 11 readr         read      small        1.07       0.431         0.3     170639360      170934272     369704960         NA
    #> 12 readr         write     small        0.942      2.76          0.525   315457536      315506688     368328704   31053850
    #> 13 nanoparquet   read      medium       0.636      0.085         0.721   168689664      168984576    1827061760         NA
    #> 14 nanoparquet   write     medium       1.46       0.183         1.65   1095286784     1095303168    1771520000  111363379
    #> 15 arrow         read      medium       0.877      0.166         0.625   178356224      178651136    2170191872         NA
    #> 16 arrow         write     medium       1.89       0.074         1.86   1100873728     1100890112    1382023168  112513786
    #> 17 duckdb        read      medium       0.917      0.229         0.716   172507136      172589056    2127544320         NA
    #> 18 duckdb        write     medium       2.64       0.336         1.46   1090650112     1090764800    3786162176  115888450
    #> 19 data.table    read      medium       2.38       0.083         0.581   167936000      168230912    1466187776         NA
    #> 20 data.table    write     medium       1.77       0.061         0.318  1100857344     1101168640    1112293376  619210198
    #> 21 data.table.gz read      medium       3.26       0.209         1.51    174243840      174538752    1544781824         NA
    #> 22 data.table.gz write     medium      21.5        0.134         3.24   1098039296     1098350592    1116880896  165242566
    #> 23 readr         read      medium      20.3        9.19          3.67    179322880      179617792    3129622528         NA
    #> 24 readr         write     medium      18.4       62.3           9.59   1092976640     1093287936    1929068544  621073998
    #> 25 nanoparquet   read      large        5.10       0.793         5.89    178274304      178569216   14240235520         NA
    #> 26 nanoparquet   write     large       14.1        1.48         15.6    8667922432     8668217344   12624773120 1113819158
    #> 27 arrow         read      large        8.28       2.91          4.85    160022528      160317440   17725652992         NA
    #> 28 arrow         write     large       19.2        0.704        19.0    8665858048     8665907200   11321851904 1125193131
    #> 29 duckdb        read      large        7.40       1.91          5.29    170344448      170639360   18960039936         NA
    #> 30 duckdb        write     large       27.4        4.04         14.8    8665448448     8665448448   20685996032 1158792810
    #> 31 data.table    read      large       21.3        0.805         4.03    173932544      174211072   13163905024         NA
    #> 32 data.table    write     large       17.6        0.576         3.02   8670298112     8670314496    8682799104 6192100558
    #> 33 data.table.gz read      large       28.2        1.48         11.4     178044928      178339840   13237747712         NA
    #> 34 data.table.gz write     large      210.         0.871        30.7    8668708864     8668708864    8691777536 1652420625
    #> 35 readr         read      large      163.        94.7          32.2     162267136      162562048   21660680192         NA
    #> 36 readr         write     large      184.       662.           95.4    8665464832     8665776128   17068998656 6210738558

Notes:

- User time (`time_user`) plus system time (`time_system`) can be larger
  than the elapsed time (`time_elapsed`) for multithreaded
  implementations and it indeed is for all tools, except for
  nanoparquet, which is single-threaded.
- All memory columns are in bytes. `mem_before` is the RSS size before
  reading/writing. `mem_max_before` is the maximum RSS size of the
  process until then. `mem_max_after` is the maximum RSS size of the
  process *after* the read/write operation.
- So I can calculate (estimate) the memory usage of the tool by
  subtracting `mem_before` from `mem_max_after`. This could overestimate
  the memory usage if `mem_max_after` were the same as `mem_max_before`,
  but this never happens in practice.
- When reading the file, `mem_max_after` includes the memory needed to
  store the data set itself. (See data sizes above.)
- For arrow, I turned off ALTREP using
  `options(arrow.use_altrep = FALSE)`, see the `benchmarks-funcs.R`
  file. Otherwise arrow does not actually read all the data into memory.

## About

See the
[`benchmark-funcs.R`](https://github.com/r-lib/nanoparquet/blob/main/vignettes/articles/benchmarks-funcs.R)
file in the nanoparquet repository for the code of the benchmarks.

I ran each measurement in its own subprocess, to make it easier to
measure memory usage.

I did *not* include the package loading time in the benchmarks.
nanoparquet has no dependencies and loads very quickly. Both the arrow
and duckdb packages might take up to 200ms to load on the test system,
because they need to load their dependencies and they are also bigger.

Show the code

``` r
if (file.exists(file.path(me, "sessioninfo.rds"))) {
  si <- readRDS(file.path(me, "sessioninfo.rds"))
} else {
  suppressPackageStartupMessages(library(arrow))
  suppressPackageStartupMessages(library(duckdb))
  suppressPackageStartupMessages(library(data.table))
  suppressPackageStartupMessages(library(readr))
  si <- sessioninfo::session_info()
  saveRDS(si, file.path(me, "sessioninfo.rds"))
}
# load sessioninfo, for the print method
suppressPackageStartupMessages(library(sessioninfo))
si
```

    #> ─ Session info ───────────────────────────────────────────────────────────────
    #>  setting  value
    #>  version  R version 4.5.3 Patched (2026-03-11 r89803)
    #>  os       macOS Tahoe 26.4
    #>  system   aarch64, darwin20
    #>  ui       X11
    #>  language (EN)
    #>  collate  C.UTF-8
    #>  ctype    C.UTF-8
    #>  tz       Europe/Madrid
    #>  date     2026-04-21
    #>  pandoc   3.8.3 @ /opt/homebrew/bin/ (via rmarkdown)
    #>  quarto   1.9.35 @ /usr/local/bin/quarto
    #>
    #> ─ Packages ───────────────────────────────────────────────────────────────────
    #>  package      * version    date (UTC) lib source
    #>  arrow        * 23.0.1.2   2026-03-25 [1] CRAN (R 4.5.2)
    #>  assertthat     0.2.1      2019-03-21 [1] CRAN (R 4.5.0)
    #>  base64enc      0.1-6      2026-02-02 [1] CRAN (R 4.5.2)
    #>  bit            4.6.0      2025-03-06 [1] CRAN (R 4.5.0)
    #>  bit64          4.6.0-1    2025-01-16 [1] CRAN (R 4.5.0)
    #>  cli            3.6.6      2026-04-09 [1] CRAN (R 4.5.3)
    #>  commonmark     2.0.0      2025-07-07 [1] CRAN (R 4.5.0)
    #>  data.table   * 1.18.2.1   2026-01-27 [1] CRAN (R 4.5.3)
    #>  DBI          * 1.3.0      2026-02-25 [1] CRAN (R 4.5.2)
    #>  digest         0.6.39     2025-11-19 [1] CRAN (R 4.5.2)
    #>  dplyr        * 1.2.1      2026-04-03 [1] CRAN (R 4.5.2)
    #>  duckdb       * 1.5.2      2026-04-13 [1] CRAN (R 4.5.2)
    #>  evaluate       1.0.5      2025-08-27 [1] CRAN (R 4.5.2)
    #>  farver         2.1.2      2024-05-13 [1] CRAN (R 4.5.0)
    #>  fastmap        1.2.0      2024-05-15 [1] CRAN (R 4.5.2)
    #>  fontawesome    0.5.3      2024-11-16 [1] CRAN (R 4.5.2)
    #>  fs             2.0.1      2026-03-24 [1] CRAN (R 4.5.2)
    #>  generics       0.1.4      2025-05-09 [1] CRAN (R 4.5.0)
    #>  ggplot2        4.0.2      2026-02-03 [1] CRAN (R 4.5.2)
    #>  glue           1.8.1      2026-04-17 [1] CRAN (R 4.5.3)
    #>  gt           * 1.3.0      2026-01-22 [1] CRAN (R 4.5.2)
    #>  gtable         0.3.6      2024-10-25 [1] CRAN (R 4.5.0)
    #>  gtExtras     * 0.6.2      2026-01-17 [1] CRAN (R 4.5.2)
    #>  hms            1.1.4      2025-10-17 [1] CRAN (R 4.5.0)
    #>  htmltools      0.5.9      2025-12-04 [1] CRAN (R 4.5.2)
    #>  htmlwidgets    1.6.4      2023-12-06 [1] CRAN (R 4.5.0)
    #>  jsonlite       2.0.0      2025-03-27 [1] CRAN (R 4.5.2)
    #>  knitr          1.51       2025-12-20 [1] CRAN (R 4.5.2)
    #>  labeling       0.4.3      2023-08-29 [1] CRAN (R 4.5.0)
    #>  lifecycle      1.0.5      2026-01-08 [1] CRAN (R 4.5.2)
    #>  litedown       0.9        2025-12-18 [1] CRAN (R 4.5.2)
    #>  magrittr       2.0.5      2026-04-04 [1] CRAN (R 4.5.2)
    #>  markdown       2.0        2025-03-23 [1] CRAN (R 4.5.0)
    #>  nanoparquet    0.5.1      2026-04-20 [1] CRAN (R 4.5.2)
    #>  nycflights13   1.0.2      2021-04-12 [1] CRAN (R 4.5.0)
    #>  otel           0.2.0.9000 2026-04-08 [1] local
    #>  paletteer      1.7.0      2026-01-08 [1] CRAN (R 4.5.2)
    #>  pillar       * 1.11.1     2025-09-17 [1] CRAN (R 4.5.2)
    #>  pkgconfig      2.0.3      2019-09-22 [1] CRAN (R 4.5.2)
    #>  prettyunits    1.2.0      2023-09-24 [1] CRAN (R 4.5.0)
    #>  purrr          1.2.1      2026-01-09 [1] CRAN (R 4.5.2)
    #>  R6             2.6.1      2025-02-15 [1] CRAN (R 4.5.2)
    #>  ragg           1.5.2      2026-03-23 [1] CRAN (R 4.5.2)
    #>  RColorBrewer   1.1-3      2022-04-03 [1] CRAN (R 4.5.0)
    #>  readr        * 2.2.0      2026-02-19 [1] CRAN (R 4.5.2)
    #>  rematch2       2.1.2      2020-05-01 [1] CRAN (R 4.5.0)
    #>  rlang          1.2.0      2026-04-06 [1] CRAN (R 4.5.2)
    #>  rmarkdown      2.31       2026-03-26 [1] CRAN (R 4.5.2)
    #>  S7             0.2.1      2025-11-14 [1] CRAN (R 4.5.2)
    #>  sass           0.4.10     2025-04-11 [1] CRAN (R 4.5.2)
    #>  scales         1.4.0      2025-04-24 [1] CRAN (R 4.5.0)
    #>  sessioninfo    1.2.3      2025-02-05 [1] CRAN (R 4.5.0)
    #>  svglite        2.2.2      2025-10-21 [1] CRAN (R 4.5.0)
    #>  systemfonts    1.3.2      2026-03-05 [1] CRAN (R 4.5.2)
    #>  textshaping    1.0.5      2026-03-06 [1] CRAN (R 4.5.2)
    #>  tibble         3.3.1      2026-01-11 [1] CRAN (R 4.5.2)
    #>  tidyselect     1.2.1      2024-03-11 [1] CRAN (R 4.5.0)
    #>  tzdb           0.5.0      2025-03-15 [1] CRAN (R 4.5.0)
    #>  utf8           1.2.6      2025-06-08 [1] CRAN (R 4.5.2)
    #>  vctrs          0.7.3      2026-04-11 [1] CRAN (R 4.5.3)
    #>  withr          3.0.2      2024-10-28 [1] CRAN (R 4.5.0)
    #>  xfun           0.57       2026-03-20 [1] CRAN (R 4.5.2)
    #>  xml2           1.5.2      2026-01-17 [1] CRAN (R 4.5.2)
    #>  yaml           2.3.12     2025-12-10 [1] CRAN (R 4.5.2)
    #>
    #>  [1] /Users/gaborcsardi/Library/R/arm64/4.5/library
    #>  [2] /Library/Frameworks/R.framework/Versions/4.5-arm64/Resources/library
    #>  * ── Packages attached to the search path.
    #>
    #> ──────────────────────────────────────────────────────────────────────────────
