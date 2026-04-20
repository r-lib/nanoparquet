#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif
#include <stddef.h>

void nanoparquet_write_stdout(const unsigned char *buf, size_t n) {
#ifdef _WIN32
    while (n > 0) {
        unsigned int chunk = n > 65536 ? 65536 : (unsigned int) n;
        int written = _write(1, buf, chunk);
        if (written <= 0) break;
        buf += written;
        n -= written;
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
