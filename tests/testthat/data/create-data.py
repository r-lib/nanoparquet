import pyarrow as pa
import pyarrow.parquet as pq
schema = pa.schema(fields=[
    pa.field(name = 'x', type = pa.int32(), nullable = False),
    pa.field(name = 'y', type = pa.int64(), nullable = False)
])
data = [ list(range(400)) * 6, list(range(400)) * 6 ]
table = pa.table(data = data, schema = schema)
pq.write_table(
  table,
  'tests/testthat/data/mixed.parquet',
  data_page_size = 400,
  dictionary_pagesize_limit = 400
)

import pyarrow as pa
import pyarrow.parquet as pq
schema = pa.schema(fields=[
    pa.field(name = 'x', type = pa.int32(), nullable = False),
    pa.field(name = 'y', type = pa.int64(), nullable = False)
])
data = [ list(range(400)) * 6, list(range(400)) * 6 ]
table = pa.table(data = data, schema = schema)
pq.write_table(
  table,
  'tests/testthat/data/mixed2.parquet',
  data_page_size = 400
)

import pyarrow as pa
import pyarrow.parquet as pq
table = pa.table({
  'x': pa.array(range(2400), type=pa.int32()),
  'y': pa.array(range(2400), type=pa.int64())
})
pq.write_table(
  table,
  'tests/testthat/data/mixed-miss.parquet',
  data_page_size = 400,
  dictionary_pagesize_limit = 400
)
