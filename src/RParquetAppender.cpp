#include "RParquetAppender.h"

RParquetAppender::RParquetAppender(
  std::string filename,
  parquet::CompressionCodec::type codec,
  int compression_level,
  std::vector<int64_t> &row_groups,
  int data_page_version,
  bool overwrite_last_row_group)
  : reader(filename, true),
    outfile(reader.pfile, codec, compression_level, row_groups),
    data_page_version(data_page_version),
    overwrite_last_row_group(overwrite_last_row_group) {
}

void RParquetAppender::init_metadata(
    SEXP dfsxp,
    SEXP dim,
    SEXP required,
    SEXP options,
    SEXP schema,
    SEXP encoding
  ) {

  std::fstream &pfile = reader.pfile;

  // set file pointer to the place where we need to write
  if (overwrite_last_row_group) {
    uint32_t nrgs = reader.file_meta_data_.row_groups.size();
    const parquet::RowGroup &last_rg = reader.file_meta_data_.row_groups[nrgs-1];
    int last_rg_size = last_rg.total_byte_size;
    // drop last row group from existing metadata, we are overwriting it
    reader.file_meta_data_.row_groups.pop_back();
    pfile.seekp(-(reader.footer_len + 8 + last_rg_size), std::ios_base::end);
  } else {
    pfile.seekp(-(reader.footer_len + 8), std::ios_base::end);
  }

  outfile.data_page_version = data_page_version;

  outfile.init_append_metadata(
    dfsxp,
    dim,
    required,
    options,
    reader.file_meta_data_.schema,
    encoding,
    reader.file_meta_data_.row_groups,
    reader.file_meta_data_.key_value_metadata
  );
}

void RParquetAppender::append() {
  outfile.append();
}
