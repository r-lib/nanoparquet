% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/nanoparquet-package.R
\name{nanoparquet-types}
\alias{nanoparquet-types}
\title{nanoparquet's type maps}
\description{
How nanoparquet maps R types to Parquet types.
}
\section{R's data types}{
When writing out a data frame, nanoparquet maps R's data types to Parquet
logical types. The following table is a summary of the mapping. For the
details see below.\tabular{llcl}{
   R type \tab Parquet type \tab Default \tab Notes \cr
   character \tab STRING (BYTE_ARRAY) \tab x \tab I.e. STRSXP. Converted to UTF-8. \cr
   " \tab BYTE_ARRAY \tab  \tab  \cr
   " \tab FIXED_LEN_BYTE_ARRAY \tab  \tab  \cr
   " \tab ENUM \tab  \tab  \cr
   " \tab UUID \tab  \tab  \cr
   Date \tab DATE \tab x \tab  \cr
   difftime \tab INT64 \tab x \tab If not hms::hms. Arrow metadata marks it as Duration(NS). \cr
   factor \tab STRING \tab x \tab Arrow metadata marks it as a factor. \cr
   " \tab ENUM \tab  \tab  \cr
   hms::hms \tab TIME(true, MILLIS) \tab x \tab Sub-milliseconds precision is lost. \cr
   integer \tab INT(32, true) \tab x \tab I.e. INTSXP. \cr
   " \tab INT64 \tab  \tab  \cr
   " \tab INT96 \tab  \tab  \cr
   " \tab DECIMAL (INT32) \tab  \tab  \cr
   " \tab DECIMAL (INT64) \tab  \tab  \cr
   " \tab INT(8, *) \tab  \tab  \cr
   " \tab INT(16, *) \tab  \tab  \cr
   " \tab INT(32, signed) \tab  \tab  \cr
   list \tab BYTE_ARRAY \tab  \tab Must be a list of raw vectors. Messing values are \code{NULL}. \cr
   " \tab FIXED_LEN_BYTE_ARRAY \tab  \tab Must be a list of raw vectors of the same length. Missing values are \code{NULL}. \cr
   logical \tab BOOLEAN \tab x \tab I.e. LGLSXP. \cr
   numeric \tab DOUBLE \tab x \tab I.e. REALSXP. \cr
   " \tab INT96 \tab  \tab  \cr
   " \tab FLOAT \tab  \tab  \cr
   " \tab DECIMAL (INT32) \tab  \tab  \cr
   " \tab DECIMAL (INT64) \tab  \tab  \cr
   " \tab INT(*, *) \tab  \tab  \cr
   " \tab FLOAT16 \tab  \tab  \cr
   POSIXct \tab TIMESTAMP(true, MICROS) \tab x \tab Sub-microsecond precision is lost. \cr
}


The non-default mappings can be selected via the \code{schema} argument. E.g.
to write out a factor column called 'name' as \code{ENUM}, use

\if{html}{\out{<div class="sourceCode r">}}\preformatted{write_parquet(..., schema = parquet_schema(name = "ENUM"))
}\if{html}{\out{</div>}}

The detailed mapping rules are listed below, in order of preference.
These rules will likely change until nanoparquet reaches version 1.0.0.
\enumerate{
\item Factors (i.e. vectors that inherit the \emph{factor} class) are converted
to character vectors using \code{as.character()}, then written as a
\code{STRSXP} (character vector) type. The fact that a column is a factor
is stored in the Arrow metadata (see below), unless the
\code{nanoparquet.write_arrow_metadata} option is set to \code{FALSE}.
\item Dates (i.e. the \code{Date} class) is written as \code{DATE} logical type, which
is an \code{INT32} type internally.
\item \code{hms} objects (from the hms package) are written as \code{TIME(true, MILLIS)}.
logical type, which is internally the \code{INT32} Parquet type.
Sub-milliseconds precision is lost.
\item \code{POSIXct} objects are written as \code{TIMESTAMP(true, MICROS)} logical type,
which is internally the \code{INT64} Parquet type.
Sub-microsecond precision is lost.
\item \code{difftime} objects (that are not \code{hms} objects, see above), are
written as an \code{INT64} Parquet type, and noting in the Arrow metadata
(see below) that this column has type \code{Duration} with \code{NANOSECONDS}
unit.
\item Integer vectors (\code{INTSXP}) are written as \code{INT(32, true)} logical type,
which corresponds to the \code{INT32} type.
\item Real vectors (\code{REALSXP}) are written as the \code{DOUBLE} type.
\item Character vectors (\code{STRSXP}) are written as the \code{STRING} logical type,
which has the \code{BYTE_ARRAY} type. They are always converted to UTF-8
before writing.
\item Logical vectors (\code{LGLSXP}) are written as the \code{BOOLEAN} type.
\item Other vectors error currently.
}

You can use \code{\link[=infer_parquet_schema]{infer_parquet_schema()}} on a data frame to map R data types
to Parquet data types.

To change the default R to Parquet mapping, use \code{\link[=parquet_schema]{parquet_schema()}} and
the \code{schema} argument of \code{\link[=write_parquet]{write_parquet()}}. Currently supported
non-default mappings are:
\itemize{
\item \code{integer} to \code{INT64},
\item \code{integer} to \code{INT96},
\item \code{double} to \code{INT96},
\item \code{double} to \code{FLOAT},
\item \code{character} to \code{BYTE_ARRAY},
\item \code{character} to \code{FIXED_LEN_BYTE_ARRAY},
\item \code{character} to \code{ENUM},
\item \code{factor} to \code{ENUM},
\item \code{integer} to \code{DECIAML} & \code{INT32},
\item \code{integer} to \code{DECIAML} & \code{INT64},
\item \code{double} to \code{DECIAML} & \code{INT32},
\item \code{double} to \code{DECIAML} & \code{INT64},
\item \code{integer} to \verb{INT(8, *)}, \verb{INT(16, *)}, \code{INT(32, signed)},
\item \code{double} to \verb{INT(*, *)},
\item \code{character} to \code{UUID},
\item \code{double} to \code{FLOAT16},
\item \code{list} of \code{raw} vectors to \code{BYTE_ARRAY},
\item \code{list} of \code{raw} vectors to \code{FIXED_LEN_BYTE_ARRAY}.
}
}

\section{Parquet's data types}{
When reading a Parquet file nanoparquet also relies on logical types and
the Arrow metadata (if present, see below) in addition to the low level
data types. The following table summarizes the mappings. See more details
below.\tabular{lll}{
   Parquet type \tab R type \tab Notes \cr
   \emph{Logical types} \tab  \tab  \cr
   BSON \tab character \tab  \cr
   DATE \tab Date \tab  \cr
   DECIMAL \tab numeric \tab REALSXP, potentially losing precision. \cr
   ENUM \tab character \tab  \cr
   FLOAT16 \tab numeric \tab REALSXP \cr
   INT(8, *) \tab integer \tab  \cr
   INT(16, *) \tab integer \tab  \cr
   INT(32, *) \tab integer \tab Large unsigned values may overflow! \cr
   INT(64, *) \tab numeric \tab REALSXP \cr
   INTERVAL \tab list(raw) \tab Missing values are \code{NULL}. \cr
   JSON \tab character \tab  \cr
   LIST \tab  \tab Not supported. \cr
   MAP \tab  \tab Not supported. \cr
   STRING \tab factor \tab If Arrow metadata says it is a factor. Also UTF8. \cr
   " \tab character \tab Otherwise. Also UTF8. \cr
   TIME \tab hms::hms \tab Also TIME_MILLIS and TIME_MICROS. \cr
   TIMESTAMP \tab POSIXct \tab Also TIMESTAMP_MILLIS and TIMESTAMP_MICROS. \cr
   UUID \tab character \tab In \code{00112233-4455-6677-8899-aabbccddeeff} form. \cr
   UNKNOWN \tab  \tab Not supported. \cr
   \emph{Primitive types} \tab  \tab  \cr
   BOOLEAN \tab logical \tab  \cr
   BYTE_ARRAY \tab factor \tab If Arrow metadata says it is a factor. \cr
   " \tab list(raw) \tab Otherwise. Missing values are \code{NULL}. \cr
   DOUBLE \tab numeric \tab REALSXP \cr
   FIXED_LEN_BYTE_ARRAY \tab list(raw) \tab Missing values are \code{NULL}. \cr
   FLOAT \tab numeric \tab REALSXP \cr
   INT32 \tab integer \tab  \cr
   INT64 \tab numeric \tab REALSXP \cr
   INT96 \tab POSIXct \tab  \cr
}


The exact rules are below. These rules will likely change until nanoparquet
reaches version 1.0.0.
\enumerate{
\item The \code{BOOLEAN} type is read as a logical vector (\code{LGLSXP}).
\item The \code{STRING} logical type and the \code{UTF8} converted type is read as
a character vector with UTF-8 encoding.
\item The \code{DATE} logical type and the \code{DATE} converted type are read as a
\code{Date} R object.
\item The \code{TIME} logical type and the \code{TIME_MILLIS} and \code{TIME_MICROS}
converted types are read as an \code{hms} object, see the hms package.
\item The \code{TIMESTAMP} logical type and the \code{TIMESTAMP_MILLIS} and
\code{TIMESTAMP_MICROS} converted types are read as \code{POSIXct} objects.
If the logical type has the \code{UTC} flag set, then the time zone of the
\code{POSIXct} object is set to \code{UTC}.
\item \code{INT32} is read as an integer vector (\code{INTSXP}).
\item \code{INT64}, \code{DOUBLE} and \code{FLOAT} are read as real vectors (\code{REALSXP}).
\item \code{INT96} is read as a \code{POSIXct} read vector with the \code{tzone} attribute
set to \code{"UTC"}. It was an old convention to store time stamps as
\code{INT96} objects.
\item The \code{DECIMAL} converted type (\code{FIXED_LEN_BYTE_ARRAY} or \code{BYTE_ARRAY}
type) is read as a real vector (\code{REALSXP}), potentially losing
precision.
\item The \code{ENUM} logical type is read as a character vector.
\item The \code{UUID} logical type is read as a character vector that uses the
\code{00112233-4455-6677-8899-aabbccddeeff} form.
\item The \code{FLOAT16} logical type is read as a real vector (\code{REALSXP}).
\item \code{BYTE_ARRAY} is read as a \emph{factor} object if the file was written
by Arrow and the original data type of the column was a factor.
(See 'The Arrow metadata below.)
\item Otherwise \code{BYTE_ARRAY} is read a list of raw vectors, with missing
values denoted by \code{NULL}.
}

Other logical and converted types are read as their annotated low level
types:
\enumerate{
\item \code{INT(8, true)}, \code{INT(16, true)} and \code{INT(32, true)} are read as
integer vectors because they are \code{INT32} internally in Parquet.
\item \code{INT(64, true)} is read as a real vector (\code{REALSXP}).
\item Unsigned integer types \code{INT(8, false)}, \code{INT(16, false)} and
\code{INT(32, false)} are read as integer vectors (\code{INTSXP}). Large
positive values may overflow into negative values, this is a known
issue that we will fix.
\item \code{INT(64, false)} is read as a real vector (\code{REALSXP}). Large
positive values may overflow into negative values, this is a known
issue that we will fix.
\item \code{INTERVAL} is a fixed length byte array, and nanoparquet reads it as
a list of raw vectors. Missing values are denoted by \code{NULL}.
\item \code{JSON} columns are read as character vectors (\code{STRSXP}).
\item \code{BSON} columns are read as raw vectors (\code{RAWSXP}).
}

These types are not yet supported:
\enumerate{
\item Nested types (\code{LIST}, \code{MAP}) are not supported.
\item The \code{UNKNOWN} logical type is not supported.
}

You can use the \code{\link[=read_parquet_schema]{read_parquet_schema()}} function to see how R would read
the columns of a Parquet file. Look at the \code{r_type} column.
}

\section{The Arrow metadata}{
Apache Arrow (i.e. the arrow R package) adds additional metadata to
Parquet files when writing them in \code{arrow::write_parquet()}. Then,
when reading the file in \code{arrow::read_parquet()}, it uses this metadata
to recreate the same Arrow and R data types as before writing.

\code{nanoparquet::write_parquet()} also adds the Arrow metadata to Parquet
files, unless the \code{nanoparquet.write_arrow_metadata} option is set to
\code{FALSE}.

Similarly, \code{nanoparquet::read_parquet()} uses the Arrow metadata in the
Parquet file (if present), unless the \code{nanoparquet.use_arrow_metadata}
option is set to FALSE.

The Arrow metadata is stored in the file level key-value metadata, with
key \code{ARROW:schema}.

Currently nanoparquet uses the Arrow metadata for two things:
\itemize{
\item It uses it to detect factors. Without the Arrow metadata factors are
read as string vectors.
\item It uses it to detect \code{difftime} objects. Without the arrow metadata
these are read as \code{INT64} columns, containing the time difference in
nanoseconds.
}
}

\seealso{
\link{nanoparquet-package} for options that modify the type
mappings.
}
