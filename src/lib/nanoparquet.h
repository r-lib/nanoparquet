#pragma once

#include "endianness.h"
#if IS_BIG_ENDIAN
#error Nanoparquet does not support big-endian platforms: https://github.com/r-lib/nanoparquet/issues/21
#endif

#include "bytebuffer.h"
#include "ParquetOutFile.h"
