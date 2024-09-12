FROM ghcr.io/r-hub/r-minimal/r-minimal:release AS build

COPY . /root/nanoparquet

RUN mkdir -p /usr/local/lib/R/site-library

RUN installr -d local::/root/nanoparquet

FROM ghcr.io/r-hub/r-minimal/r-minimal:release AS final

COPY --from=build /usr/local/lib/R/site-library /usr/local/lib/R/site-library

RUN R -q -e 'library(nanoparquet); write_parquet(mtcars, "/tmp/mtcars.parquet"); read_parquet("/tmp/mtcars.parquet")' && \
    rm -rf /tmp/Rtmp*
