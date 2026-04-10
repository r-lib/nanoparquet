
def make_nested_list_parquet(filename, depth, rows=None):
  """
  Write a Parquet file with a single column 'a' that is a list nested to
  `depth` levels (e.g. depth=1 -> list<int32>, depth=2 -> list<list<int32>>).

  Args:
    filename: output .parquet path
    depth:    nesting depth, must be >= 1
    rows:     list of row values; if None a small default example is used.
              Each row must be a Python list nested to `depth` levels with
              int values at the leaves.
  """
  import pyarrow as pa
  import pyarrow.parquet as pq

  if depth < 1:
    raise ValueError("depth must be >= 1")

  # Build the Arrow type recursively: list<list<...<int32>...>>
  def make_type(d):
    return pa.int32() if d == 0 else pa.list_(make_type(d - 1))

  col_type = make_type(depth)

  # Default data: 3 rows that exercise empty lists at every level
  if rows is None:
    def make_rows(d):
      if d == 1:
        return [
          [1, 2, 3],  # normal
          [],         # empty
          [4],        # singleton
        ]
      inner = make_rows(d - 1)
      return [
        [inner[0], inner[1]],  # one non-empty + one empty sub-list
        [],                    # empty outer list
        [inner[2]],            # singleton sub-list
      ]
    rows = make_rows(depth)

  schema = pa.schema([pa.field('a', col_type)])
  table = pa.table({'a': pa.array(rows, type=col_type)}, schema=schema)
  pq.write_table(table, filename)


def make_list_parquet(
  filename,
  list_nullable=True,
  element_nullable=True,
  rows=None,
  data_page_version="1.0"
):
  """
  Write a Parquet file with a single list<int32> column 'a'.

  Args:
    filename:           output .parquet path
    list_nullable:      if True, the outer list field is OPTIONAL (may be null)
    element_nullable:   if True, list elements are OPTIONAL (may be null)
    rows:               list of row values (list of lists of ints); if None a
                        small default example is used
    data_page_version:  "1.0" (default) or "2.0"
  """
  import pyarrow as pa
  import pyarrow.parquet as pq

  elem_field = pa.field("item", pa.int32(), nullable=element_nullable)
  list_type = pa.list_(elem_field)

  if rows is None:
    rows = [[1, 2, 3], [], [4]]

  col_field = pa.field("a", list_type, nullable=list_nullable)
  schema = pa.schema([col_field])
  arr = pa.array(rows, type=list_type)
  table = pa.table({"a": arr}, schema=schema)
  pq.write_table(table, filename, data_page_version=data_page_version)


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
