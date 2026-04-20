#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif
#include <stddef.h>

void nanoparquet_write_stdout(const unsigned char *buf, size_t n) {
#ifdef _WIN32
    _write(1, buf, (unsigned int) n);
#else
    write(1, buf, n);
#endif
}
