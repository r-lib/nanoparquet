
def do_float():
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

def do_mixed():
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
  import random
  schema = pa.schema(fields=[
      pa.field(name = 'x', type = pa.int32()),
      pa.field(name = 'y', type = pa.int64()),
      pa.field(name = "s", type = pa.utf8()),
      pa.field(name = 'f', type = pa.float32()),
      pa.field(name = 'd', type = pa.float64()),
      pa.field(name = "i96", type = pa.timestamp('ms', tz='UTC')),
  ])
  data = [
    list(range(2400)),
    list(range(2400)),
    [ str(x) for x in range(2400) ],
    list(range(2400)),
    list(range(2400)),
    [ pa.scalar(datetime(x, 1, 1), type=pa.timestamp('ms', tz='UTC'))
        for x in range(1, 2401) ],
  ]

  for col in range(len(data)):
    for i in range(20):
      data[col][random.randint(0, 2400-1)] = None

  table = pa.table(data = data, schema = schema)
  pq.write_table(
    table,
    'tests/testthat/data/mixed-miss.parquet',
    use_deprecated_int96_timestamps = True,
    data_page_size = 400,
    dictionary_pagesize_limit = 400
  )

def do_decimal():
  import pyarrow as pa
  import pyarrow.parquet as pq
  import random
  random.seed(10)
  fields = [
      pa.field(name = 'dba', type = pa.decimal128(7), nullable = False),
      pa.field(name = 'dbam', type = pa.decimal128(7)),
  ]
  schema = pa.schema(fields = fields)
  data = [
    list(range(400)) * 3,
    list(range(400)) * 3,
  ]
  for i in range(10):
    data[1][random.randint(0, 1200-1)] = None

  table = pa.table(data = data, schema = schema)
  pq.write_table(
    table,
    'tests/testthat/data/decimal.parquet',
    data_page_size = 400,
    dictionary_pagesize_limit = 400
  )

  fields2 = fields + [
      pa.field(name = 'di64', type = pa.decimal128(11), nullable = False),
      pa.field(name = 'di64m', type = pa.decimal128(11)),
  ]
  schema2 = pa.schema(fields = fields2)
  data2 = data + data
  table2 = pa.table(data = data2, schema = schema2)
  pq.write_table(
    table2,
    'tests/testthat/data/decimal2.parquet',
    store_decimal_as_integer = True,
    data_page_size = 400,
    dictionary_pagesize_limit = 400
  )

def do_binary():
  import pyarrow as pa
  import pyarrow.parquet as pq
  import random
  random.seed(10)
  fields = [
      pa.field(name = 'ba', type = pa.binary(), nullable = False),
      pa.field(name = 'bam', type = pa.binary()),
  ]
  schema = pa.schema(fields = fields)
  data = [
    [ str(x) for x in range(400) ] * 3,
    [ str(x) for x in range(400) ] * 3,
  ]
  for i in range(10):
    data[1][random.randint(0, 1200-1)] = None

  table = pa.table(data = data, schema = schema)
  pq.write_table(
    table,
    'tests/testthat/data/binary.parquet',
    data_page_size = 400,
    dictionary_pagesize_limit = 400
  )

def do_uuid():
  import pyarrow as pa
  import pyarrow.parquet as pq
  import random
  import uuid
  random.seed(10)
  fields = [
      pa.field(name = 'ba', type = pa.uuid(), nullable = False),
      pa.field(name = 'bam', type = pa.uuid()),
  ]
  schema = pa.schema(fields = fields)
  data = [
    [ uuid.uuid4().bytes for x in range(400) ] * 3,
    [ uuid.uuid4().bytes for x in range(400) ] * 3,
  ]
  for i in range(10):
    data[1][random.randint(0, 1200-1)] = None

  table = pa.table(data = data, schema = schema)
  pq.write_table(
    table,
    'tests/testthat/data/uuid.parquet',
    version='2.6',
    data_page_size = 400,
    dictionary_pagesize_limit = 400
  )

def do_float16():
  import pyarrow as pa
  import pyarrow.parquet as pq
  import random
  import numpy as np
  random.seed(10)
  fields = [
      pa.field(name = 'dba', type = pa.float16(), nullable = False),
      pa.field(name = 'dbam', type = pa.float16()),
  ]
  schema = pa.schema(fields = fields)
  data = [
    np.array(list(range(400)) * 3, dtype=np.float16),
    np.array(list(range(400)) * 3, dtype=np.float16)
  ]
  for i in range(10):
    p = random.randint(0, 1200-1)
    print(p)
    data[1][p] = None

  table = pa.table(data = data, schema = schema)
  pq.write_table(
    table,
    'tests/testthat/data/float16.parquet',
    data_page_size = 400,
    dictionary_pagesize_limit = 400
  )

if __name__ == "__main__":
  import sys
  if len(sys.argv) == 1:
    do_float()
    do_mixed()
    do_decimal()
    do_binary()
    do_uuid()
    do_float16()
  elif sys.argv[1] == 'float':
    do_float()
  elif sys.argv[1] == 'mixed':
    do_mixed()
  elif sys.argv[1] == 'decimal':
    do_decimal()
  elif sys.argv[1] == 'binary':
    do_binary()
  elif sys.argv[1] == 'uuid':
    do_uuid()
  elif sys.argv[1] == 'float16':
    do_float16()
