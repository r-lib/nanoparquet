#include <cstdint>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

#include <Rinternals.h>
#undef TYPE_BITS

#include "RParquetReader.h"
#include "unwind.h"

#ifdef _WIN32
  #include <io.h>
  #include <fcntl.h>
#else
  #include <unistd.h>
#endif

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

extern SEXP nanoparquet_call;

static int nq_truncate(const std::string &fname, int64_t size) {
#ifdef _WIN32
  int fd = _open(fname.c_str(), _O_RDWR | _O_BINARY);
  if (fd == -1) return -1;
  int ret = (int)_chsize_s(fd, (__int64)size);
  _close(fd);
  return ret;
#else
  return truncate(fname.c_str(), (off_t)size);
#endif
}

extern "C" {

SEXP rf_nanoparquet_edit_metadata(
  SEXP filesxp,
  SEXP keyssxp,
  SEXP valuessxp
) noexcept {
  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_errorcall(nanoparquet_call,
      "nanoparquet_edit_metadata: filename must be a string");
  }
  const char *cfname = CHAR(STRING_ELT(filesxp, 0));
  std::string fname = cfname;

  CPP_INIT;
  CPP_BEGIN;

  // Step 1: Read existing metadata (read-only)
  parquet::FileMetaData fmd;
  int32_t footer_len;
  int64_t file_size;
  {
    RParquetReader reader(fname);
    fmd = reader.file_meta_data_;
    footer_len = reader.footer_len;
    reader.pfile.seekg(0, std::ios_base::end);
    file_size = (int64_t)reader.pfile.tellg();
    // reader destructor closes the file
  }

  // Step 2: Build the new key-value metadata vector
  R_xlen_t nkv = Rf_xlength(keyssxp);
  std::vector<parquet::KeyValue> new_kv;
  new_kv.reserve((size_t)nkv);
  for (R_xlen_t i = 0; i < nkv; i++) {
    parquet::KeyValue kv;
    kv.__set_key(std::string(CHAR(STRING_ELT(keyssxp, i))));
    SEXP val = STRING_ELT(valuessxp, i);
    if (val != NA_STRING) {
      kv.__set_value(std::string(CHAR(val)));
    }
    new_kv.push_back(kv);
  }
  fmd.__set_key_value_metadata(new_kv);

  // Step 3: Serialize the modified FileMetaData with Thrift compact encoding
  std::shared_ptr<TMemoryBuffer> mem_buffer(
    new TMemoryBuffer(static_cast<uint32_t>(1024 * 1024))
  );
  TCompactProtocolFactoryT<TMemoryBuffer> tproto_factory;
  std::shared_ptr<TProtocol> tproto = tproto_factory.getProtocol(mem_buffer);
  fmd.write(tproto.get());

  uint8_t *out_buffer;
  uint32_t out_length;
  mem_buffer->getBuffer(&out_buffer, &out_length);

  // Step 4: Open file in read-write mode (without truncation)
  std::fstream pfile;
  pfile.open(fname, std::ios::binary | std::ios::in | std::ios::out);
  if (!pfile.is_open()) {
    throw std::runtime_error("Cannot open Parquet file for writing: " + fname);
  }

  // Step 5: Seek to the start of the existing footer.
  // Parquet file layout: [PAR1][...data...][footer_data][footer_len:4][PAR1]
  // footer_len covers only the footer_data bytes; add 8 for the length field
  // and the trailing magic bytes.
  pfile.seekp(-(footer_len + 8), std::ios_base::end);

  // Step 6: Write the new serialized footer
  pfile.write(reinterpret_cast<char *>(out_buffer), out_length);

  // Step 7: Write the 4-byte footer length
  pfile.write(reinterpret_cast<const char *>(&out_length), 4);

  // Step 8: Write the trailing magic bytes
  pfile.write("PAR1", 4);
  pfile.flush();

  int64_t new_size = (int64_t)pfile.tellp();
  pfile.close();

  // Step 9: Truncate if the new footer is shorter than the old one
  if (new_size < file_size) {
    if (nq_truncate(fname, new_size) != 0) {
      throw std::runtime_error("Cannot truncate Parquet file: " + fname);
    }
  }

  CPP_END;

  return R_NilValue;
}

} // extern "C"
