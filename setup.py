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
include_paths = ['src', 'src/lib', 'src/thrift', 'src/zstd/include']
toolchain_args = ['-std=c++11']
if platform.system() == 'Darwin':
    toolchain_args.extend(['-stdlib=libc++', '-mmacosx-version-min=10.13'])

def get_files(dirname):
	file_list = os.listdir(dirname)
	result = []
	for fname in file_list:
		if fname in ignored_files:
			continue
		full_name = os.path.join(dirname, fname)
		if os.path.isdir(full_name):
			result += get_files(full_name)
		else:
			if full_name.endswith('.cpp') or full_name.endswith('.cc'):
				result.append(full_name)
	return result

libnanoparquet = Extension(
  'nanoparquet',
  sources = get_files('src'),
	include_dirs = include_paths,
	extra_compile_args=toolchain_args,
	extra_link_args=toolchain_args,
	language = 'c++'
)

setup (
  name = 'nanoparquet',
  version = '0.3.0.9000',
  description = 'Nanoparquet',
  ext_modules = [libnanoparquet]
)
