/**
 * @file otlp2records.h
 * @brief C FFI bindings for otlp2records - Transform OTLP telemetry to Arrow
 *
 * This header provides C-compatible bindings for the otlp2records Rust library.
 * It uses the Arrow C Data Interface for passing Arrow data across the FFI boundary.
 */

#ifndef OTLP2RECORDS_H
#define OTLP2RECORDS_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Arrow C Data Interface
 * https://arrow.apache.org/docs/format/CDataInterface.html
 * ============================================================================ */

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
    int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
    const char* (*get_last_error)(struct ArrowArrayStream*);
    void (*release)(struct ArrowArrayStream*);
    void* private_data;
};

#endif /* ARROW_C_STREAM_INTERFACE */

/* ============================================================================
 * otlp2records Types
 * ============================================================================ */

/**
 * @brief OTLP signal types supported by the parser.
 */
typedef enum OtlpSignalType {
    /** Log records */
    OTLP_SIGNAL_LOGS = 0,
    /** Trace spans */
    OTLP_SIGNAL_TRACES = 1,
    /** Gauge metrics */
    OTLP_SIGNAL_METRICS_GAUGE = 2,
    /** Sum/counter metrics */
    OTLP_SIGNAL_METRICS_SUM = 3,
    /** Histogram metrics */
    OTLP_SIGNAL_METRICS_HISTOGRAM = 4,
    /** Exponential histogram metrics */
    OTLP_SIGNAL_METRICS_EXP_HISTOGRAM = 5,
} OtlpSignalType;

/**
 * @brief Input format for OTLP data.
 */
typedef enum OtlpInputFormat {
    /** Auto-detect format from content */
    OTLP_FORMAT_AUTO = 0,
    /** Binary protobuf format */
    OTLP_FORMAT_PROTOBUF = 1,
    /** JSON format */
    OTLP_FORMAT_JSON = 2,
    /** Newline-delimited JSON */
    OTLP_FORMAT_JSONL = 3,
} OtlpInputFormat;

/**
 * @brief Status codes returned by FFI functions.
 */
typedef enum OtlpStatus {
    /** Success */
    OTLP_OK = 0,
    /** Invalid argument (null pointer, invalid enum value, etc.) */
    OTLP_ERROR_INVALID_ARGUMENT = 1,
    /** Parse failed (invalid OTLP data) */
    OTLP_ERROR_PARSE_FAILED = 2,
    /** Invalid format (format detection failed) */
    OTLP_ERROR_INVALID_FORMAT = 3,
    /** Out of memory */
    OTLP_ERROR_OUT_OF_MEMORY = 4,
    /** Internal error (panic caught, unexpected state) */
    OTLP_ERROR_INTERNAL = 5,
} OtlpStatus;

/**
 * @brief Opaque parser handle for streaming OTLP data.
 *
 * This handle maintains state for parsing OTLP data and producing Arrow batches.
 * It is NOT thread-safe - use one handle per thread.
 */
typedef struct OtlpParserHandle OtlpParserHandle;

/* ============================================================================
 * Parser Lifecycle
 * ============================================================================ */

/**
 * @brief Create a new streaming parser for the given signal type.
 *
 * @param signal_type The OTLP signal type (logs, traces, metrics_gauge, etc.)
 * @param format Input format hint (OTLP_FORMAT_AUTO for auto-detection)
 * @param out_handle Output: pointer to created parser handle
 * @return OTLP_OK on success, error code otherwise
 *
 * @note Caller owns the handle and must call otlp_parser_destroy().
 */
OtlpStatus otlp_parser_create(
    OtlpSignalType signal_type,
    OtlpInputFormat format,
    OtlpParserHandle** out_handle
);

/**
 * @brief Destroy parser and release all resources.
 *
 * @param handle Parser handle (may be NULL, which is a no-op)
 */
void otlp_parser_destroy(OtlpParserHandle* handle);

/* ============================================================================
 * Streaming Interface
 * ============================================================================ */

/**
 * @brief Push input bytes to the parser.
 *
 * @param handle Parser handle
 * @param data Pointer to input bytes (JSON or protobuf)
 * @param len Length of input bytes
 * @param is_final Non-zero if this is the last chunk (triggers parsing)
 * @return OTLP_OK on success, error code otherwise
 *
 * @note Caller retains ownership of data buffer.
 * @note For JSONL: Each complete line is parsed immediately.
 * @note For JSON/protobuf: Data is buffered until is_final=true.
 */
OtlpStatus otlp_parser_push(
    OtlpParserHandle* handle,
    const uint8_t* data,
    size_t len,
    int is_final
);

/**
 * @brief Export available batches as an ArrowArrayStream.
 *
 * @param handle Parser handle
 * @param out_stream Output: ArrowArrayStream to populate
 * @return OTLP_OK on success, error code otherwise
 *
 * @note Caller must call out_stream->release() when done.
 * @note Stream is valid until next push() or destroy() on handle.
 * @note Stream may yield 0 batches if no data was parsed.
 */
OtlpStatus otlp_parser_drain(
    OtlpParserHandle* handle,
    struct ArrowArrayStream* out_stream
);

/* ============================================================================
 * Schema Access
 * ============================================================================ */

/**
 * @brief Get the Arrow schema for a signal type.
 *
 * @param signal_type The OTLP signal type
 * @param out_schema Output: ArrowSchema to populate
 * @return OTLP_OK on success, error code otherwise
 *
 * @note Caller must call out_schema->release() when done.
 */
OtlpStatus otlp_get_schema(
    OtlpSignalType signal_type,
    struct ArrowSchema* out_schema
);

/* ============================================================================
 * One-Shot Convenience API
 * ============================================================================ */

/**
 * @brief Transform OTLP bytes to Arrow in one call (non-streaming).
 *
 * @param signal_type The OTLP signal type
 * @param format Input format (OTLP_FORMAT_AUTO for auto-detection)
 * @param data Input bytes
 * @param len Length of input bytes
 * @param out_array Output: ArrowArray with the batch data
 * @param out_schema Output: ArrowSchema for the batch
 * @return OTLP_OK on success, error code otherwise
 *
 * @note Caller must call out_array->release() and out_schema->release().
 * @note Array and schema are independent (can release in any order).
 */
OtlpStatus otlp_transform(
    OtlpSignalType signal_type,
    OtlpInputFormat format,
    const uint8_t* data,
    size_t len,
    struct ArrowArray* out_array,
    struct ArrowSchema* out_schema
);

/* ============================================================================
 * Error Handling
 * ============================================================================ */

/**
 * @brief Get the last error message for a parser handle.
 *
 * @param handle Parser handle (may be NULL)
 * @return Error message string, or NULL if no error
 *
 * @note Returned string is valid until next FFI call on same handle.
 */
const char* otlp_parser_last_error(const OtlpParserHandle* handle);

/**
 * @brief Get a static message for a status code.
 *
 * @param status Status code
 * @return Static string describing the status (never NULL)
 */
const char* otlp_status_message(OtlpStatus status);

#ifdef __cplusplus
}
#endif

#endif /* OTLP2RECORDS_H */
