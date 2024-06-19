#!usr/bin/env python

from setuptools import setup, Extension
import os
import platform
import sys

###########################################################################

# Check Python's version info and exit early if it is too old
if sys.version_info < (3, 8):
    print("This module requires Python >= 3.8")
    sys.exit(0)

ignored_files = [
  'arrow-schema.cpp',
  'base64.cpp',
  'dictionary-encoding.cpp',
  'encodings.cpp',
  'protect.cpp',
  'r-base64.cpp',
  'read-metadata.cpp',
  'read-pages.cpp',
  'read.cpp',
  'rwrapper.cpp',
  'snappy.cpp',
  'test.cpp',
  'write.cpp'
]
extensions = ['.cpp', '.cc']
include_paths = [
  'src',
  'src/lib',
  'src/thrift',
  'src/zstd/include'
]
toolchain_args = ['-std=c++11']
if platform.system() == 'Darwin':
    toolchain_args.extend(['-stdlib=libc++', '-mmacosx-version-min=10.13'])

libnanoparquet = Extension(
  'nanoparquet',
  sources = [
    "src/pywrapper.cpp",

    "src/lib/ParquetFile.cpp",
    "src/lib/ParquetOutFile.cpp",
    "src/lib/RleBpDecoder.cpp",

    "src/parquet/parquet_types.cpp",

    "src/thrift/protocol/TProtocol.cpp",
    "src/thrift/transport/TTransportException.cpp",
    "src/thrift/transport/TBufferTransports.cpp",

    "src/fastpforlib/bitpacking.cpp",

    "src/snappy/snappy.cc",
    "src/snappy/snappy-sinksource.cc",

    "src/miniz/miniz.cpp",

    "src/zstd/common/entropy_common.cpp",
    "src/zstd/common/error_private.cpp",
    "src/zstd/common/fse_decompress.cpp",
    "src/zstd/common/xxhash.cpp",
    "src/zstd/common/zstd_common.cpp",
    "src/zstd/decompress/huf_decompress.cpp",
    "src/zstd/decompress/zstd_ddict.cpp",
    "src/zstd/decompress/zstd_decompress.cpp",
    "src/zstd/decompress/zstd_decompress_block.cpp",
    "src/zstd/compress/fse_compress.cpp",
    "src/zstd/compress/hist.cpp",
    "src/zstd/compress/huf_compress.cpp",
    "src/zstd/compress/zstd_compress.cpp",
    "src/zstd/compress/zstd_compress_literals.cpp",
    "src/zstd/compress/zstd_compress_sequences.cpp",
    "src/zstd/compress/zstd_compress_superblock.cpp",
    "src/zstd/compress/zstd_double_fast.cpp",
    "src/zstd/compress/zstd_fast.cpp",
    "src/zstd/compress/zstd_lazy.cpp",
    "src/zstd/compress/zstd_ldm.cpp",
    "src/zstd/compress/zstd_opt.cpp"
  ],
	include_dirs = include_paths,
	extra_compile_args = toolchain_args,
	extra_link_args = toolchain_args,
	language = 'c++'
)

setup (
  name = 'nanoparquet',
  version = '0.3.0.9000',
  description = 'Nanoparquet',
  ext_modules = [libnanoparquet]
)
