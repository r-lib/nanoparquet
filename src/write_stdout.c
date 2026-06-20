#ifdef _WIN32
#include <io.h>
#include <fcntl.h>
#else
#include <unistd.h>
#endif
#include <stddef.h>

void nanoparquet_write_stdout(const unsigned char *buf, size_t n) {
#ifdef _WIN32
    // stdout defaults to text mode on Windows, which would translate
    // \n (0x0A) bytes to \r\n and corrupt the binary Parquet output.
    int oldmode = _setmode(1, _O_BINARY);
    while (n > 0) {
        unsigned int chunk = n > 65536 ? 65536 : (unsigned int) n;
        int written = _write(1, buf, chunk);
        if (written <= 0) break;
        buf += written;
        n -= written;
    }
    if (oldmode != -1) {
        _setmode(1, oldmode);
    }
#else
    while (n > 0) {
        ssize_t written = write(1, buf, n);
        if (written <= 0) break;
        buf += (size_t) written;
        n -= (size_t) written;
    }
#endif
}
