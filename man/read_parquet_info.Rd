% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/parquet-metadata.R
\name{read_parquet_info}
\alias{read_parquet_info}
\alias{parquet_info}
\title{Short summary of a Parquet file}
\usage{
read_parquet_info(file)

parquet_info(file)
}
\arguments{
\item{file}{Path to a Parquet file.}
}
\value{
Data frame with columns:
\itemize{
\item \code{file_name}: file name.
\item \code{num_cols}: number of (leaf) columns.
\item \code{num_rows}: number of rows.
\item \code{num_row_groups}: number of row groups.
\item \code{file_size}: file size in bytes.
\item \code{parquet_version}: Parquet version.
\item \code{created_by}: A string scalar, usually the name of the software
that created the file. \code{NA} if not available.
}
}
\description{
Short summary of a Parquet file
}
\seealso{
\code{\link[=read_parquet_metadata]{read_parquet_metadata()}} to read more metadata,
\code{\link[=read_parquet_schema]{read_parquet_schema()}} for column information.
\code{\link[=read_parquet]{read_parquet()}}, \code{\link[=write_parquet]{write_parquet()}}, \link{nanoparquet-types}.
}
