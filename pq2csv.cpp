#include <iostream>
#include <cmath>

#include "miniparquet.h"

using namespace miniparquet;
using namespace std;

// surely they are joking
constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
constexpr int64_t kMillisecondsInADay = 86400000LL;
constexpr int64_t kNanosecondsInADay = kMillisecondsInADay * 1000LL * 1000LL;

static int64_t impala_timestamp_to_nanoseconds(const Int96 &impala_timestamp) {
	int64_t days_since_epoch = impala_timestamp.value[2]
			- kJulianToUnixEpochDays;
	int64_t nanoseconds =
			*(reinterpret_cast<const int64_t*>(&(impala_timestamp.value)));
	return days_since_epoch * kNanosecondsInADay + nanoseconds;
}

int main(int argc, char *const argv[]) {

	for (int arg = 1; arg < argc; arg++) {
		auto f = ParquetFile(argv[arg]);

		ResultChunk rc;
		ScanState s;

		f.initialize_result(rc);

		// TODO check for NULLness

		while (f.scan(s, rc)) {
			for (uint64_t row = 0; row < rc.nrows; row++) {
				for (auto &col : rc.cols) {
					if (!((uint8_t*) col.defined.ptr)[row]) {
						printf("NULL");
						if (col.id < rc.cols.size() - 1) {
							printf("\t");
						}
						continue;
					}
					switch (col.col->type) {
					case parquet::format::Type::BOOLEAN:
						printf("%s",
								((bool*) col.data.ptr)[row] ? "True" : "False");
						break;

					case parquet::format::Type::INT32:
						printf("%d", ((int32_t*) col.data.ptr)[row]);
						break;
					case parquet::format::Type::INT64:
						printf("%lld", ((int64_t*) col.data.ptr)[row]);
						break;
					case parquet::format::Type::INT96: {
						// TODO when is this a timestamp?
						auto val = ((Int96*) col.data.ptr)[row];
						time_t a = impala_timestamp_to_nanoseconds(val)
								/ 1000000000;
						char buffer[80];
						strftime(buffer, 80, "%Y-%m-%d %H:%M:%S", gmtime(&a));
						printf("%s", buffer);
						break;
					}
					case parquet::format::Type::FLOAT:
						printf("%f", ((float*) col.data.ptr)[row]);
						break;
					case parquet::format::Type::DOUBLE:
						printf("%lf", ((double*) col.data.ptr)[row]);
						break;
					case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: {
						auto &s_ele = col.col->schema_element;

						if (!s_ele->__isset.converted_type) {
							throw runtime_error("Invalid flba type");

						}

						// TODO what about logical_type??
						switch (col.col->schema_element->converted_type) {
						case parquet::format::ConvertedType::DECIMAL: { // this is a giant mess
							auto type_len = s_ele->type_length;
							auto bytes = ((char**) col.data.ptr)[row];
							int64_t val = 0;
							for (auto i = 0; i < type_len; i++) {
								val = val << ((type_len - i) * 8)
										| (uint8_t) bytes[i];
							}
							printf("%.2f", val / pow(10.0, s_ele->scale));

						}
							break;
						default:
							throw runtime_error("Invalid flba type");

						}

					}
						break;
					case parquet::format::Type::BYTE_ARRAY:
						printf("%s", ((char**) col.data.ptr)[row]);

						break;

					default:
						throw runtime_error("Invalid type");
						break;

					}
					if (col.id < rc.cols.size() - 1) {
						printf("\t");
					}

				}
				printf("\n");
			}

		}

	}

}
