import pyarrow as pa
import pyarrow.parquet as pq
schema = pa.schema(fields=[
    pa.field(name = "f", type = pa.float32(), nullable = False),
])
data = [
  list(range(400)) * 10,
]
table = pa.table(data = data, schema = schema)
pq.write_table(
  table,
  'tests/testthat/data/float.parquet',
  row_group_size = 1500,
  data_page_size = 400,
  use_dictionary = False
)

import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
schema = pa.schema(fields=[
    pa.field(name = 'x', type = pa.int32(), nullable = False),
    pa.field(name = 'y', type = pa.int64(), nullable = False),
    pa.field(name = "s", type = pa.utf8(), nullable = False),
    pa.field(name = 'f', type = pa.float32(), nullable = False),
    pa.field(name = 'd', type = pa.float64(), nullable = False),
    pa.field(name = "i96", type = pa.timestamp('ms', tz='UTC'), nullable = False),
])
data = [
  list(range(400)) * 6,
  list(range(400)) * 6,
  [ str(x) for x in range(400) ] * 6,
  list(range(400)) * 6,
  list(range(400)) * 6,
  [ pa.scalar(datetime(x, 1, 1), type=pa.timestamp('ms', tz='UTC'))
    for x in range(1800, 2200) ] * 6,
]
table = pa.table(data = data, schema = schema)
pq.write_table(
  table,
  'tests/testthat/data/mixed.parquet',
  use_deprecated_int96_timestamps = True,
  data_page_size = 400,
  dictionary_pagesize_limit = 400
)

pq.write_table(
  table,
  'tests/testthat/data/mixed2.parquet',
  use_deprecated_int96_timestamps = True,
  data_page_size = 400
)

import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
table = pa.table({
  'x': pa.array(range(2400), type=pa.int32()),
  'y': pa.array(range(2400), type=pa.int64()),
  's': pa.array([ str(x) for x in range(2400) ], type=pa.utf8()),
  'f': pa.array(range(2400), type=pa.float32()),
  'd': pa.array(range(2400), type=pa.float64()),
  'i96': pa.array(
    [ pa.scalar(datetime(x, 1, 1), type=pa.timestamp('ms', tz='UTC'))
      for x in range(1, 2401) ],
    type = pa.timestamp('ms', tz='UTC')
  ),
})
pq.write_table(
  table,
  'tests/testthat/data/mixed-miss.parquet',
  use_deprecated_int96_timestamps = True,
  data_page_size = 400,
  dictionary_pagesize_limit = 400
)
