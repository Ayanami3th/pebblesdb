#pragma once
#include <stdint.h>

#include <chrono>
#include <cstdarg>
#include <functional>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <map>
#include <sys/types.h>
#include "pebblesdb/status.h"
#include "pebblesdb/env.h"

namespace leveldb
{
  // DEPRECATED
  // Priority of an IO request. This is a hint and does not guarantee any
  // particular QoS.
  // IO_LOW - Typically background reads/writes such as compaction/flush
  // IO_HIGH - Typically user reads/synchronous WAL writes
  enum class IOPriority : uint8_t 
  {
    kIOLow,
    kIOHigh,
    kIOTotal,
  };

  // Type of the data begin read/written. It can be passed down as a flag
  // for the FileSystem implementation to optionally handle different types in
  // different ways
  enum class IOType : uint8_t 
  {
    kData,
    kFilter,
    kIndex,
    kMetadata,
    kWAL,
    kManifest,
    kLog,
    kUnknown,
    kInvalid,
  };

  // These values match Linux definition
  // https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/include/uapi/linux/fcntl.h#n56
  enum WriteLifeTimeHint
  {
    WLTH_NOT_SET = 0, // No hint information set
    WLTH_NONE,        // No hints about write life time
    WLTH_SHORT,       // Data written has a short life time
    WLTH_MEDIUM,      // Data written has a medium life time
    WLTH_LONG,        // Data written has a long life time
    WLTH_EXTREME,     // Data written has an extremely long life time
  };

  // Per-request options that can be passed down to the FileSystem
  // implementation. These are hints and are not necessarily guaranteed to be
  // honored. More hints can be added here in the future to indicate things like
  // storage media (HDD/SSD) to be used, replication level etc.
  struct IOOptions
  {
    // Timeout for the operation in microseconds
    std::chrono::microseconds timeout;

    // DEPRECATED
    // Priority - high or low
    IOPriority prio;

    // Priority used to charge rate limiter configured in file system level (if
    // any)
    // Limitation: right now RocksDB internal does not consider this
    // rate_limiter_priority
    // Env::IOPriority rate_limiter_priority;

    // Type of data being read/written
    IOType type;

    // EXPERIMENTAL
    // An option map that's opaque to RocksDB. It can be used to implement a
    // custom contract between a FileSystem user and the provider. This is only
    // useful in cases where a RocksDB user directly uses the FileSystem or file
    // object for their own purposes, and wants to pass extra options to APIs
    // such as NewRandomAccessFile and NewWritableFile.
    std::unordered_map<std::string, std::string> property_bag;

    // Force directory fsync, some file systems like btrfs may skip directory
    // fsync, set this to force the fsync
    bool force_dir_fsync;

    IOOptions() : IOOptions(false) {}

    explicit IOOptions(bool force_dir_fsync_)
        : timeout(std::chrono::microseconds::zero()),
          prio(IOPriority::kIOLow),
          // rate_limiter_priority(Env::IO_TOTAL),
          type(IOType::kUnknown),
          force_dir_fsync(force_dir_fsync_) {}
  };

  struct DirFsyncOptions
  {
    enum FsyncReason : uint8_t
    {
      kNewFileSynced,
      kFileRenamed,
      kDirRenamed,
      kFileDeleted,
      kDefault,
    } reason;

    std::string renamed_new_name; // for kFileRenamed
    // add other options for other FsyncReason

    DirFsyncOptions();

    explicit DirFsyncOptions(std::string file_renamed_new_name);

    explicit DirFsyncOptions(FsyncReason fsync_reason);
  };

  // A structure to pass back some debugging information from the FileSystem
  // implementation to RocksDB in case of an IO error
  struct IODebugContext
  {
    // file_path to be filled in by RocksDB in case of an error
    std::string file_path;

    // A map of counter names to values - set by the FileSystem implementation
    std::map<std::string, uint64_t> counters;

    // To be set by the FileSystem implementation
    std::string msg;

    // To be set by the underlying FileSystem implementation.
    std::string request_id;

    // In order to log required information in IO tracing for different
    // operations, Each bit in trace_data stores which corresponding info from
    // IODebugContext will be added in the trace. Foreg, if trace_data = 1, it
    // means bit at position 0 is set so TraceData::kRequestID (request_id) will
    // be logged in the trace record.
    //
    enum TraceData : char
    {
      // The value of each enum represents the bitwise position for
      // that information in trace_data which will be used by IOTracer for
      // tracing. Make sure to add them sequentially.
      kRequestID = 0,
    };
    uint64_t trace_data = 0;

    IODebugContext() {}

    void AddCounter(std::string &name, uint64_t value)
    {
      counters.emplace(name, value);
    }

    // Called by underlying file system to set request_id and log request_id in
    // IOTracing.
    void SetRequestId(const std::string &_request_id)
    {
      request_id = _request_id;
      trace_data |= (1 << TraceData::kRequestID);
    }

    std::string ToString()
    {
      std::ostringstream ss;
      ss << file_path << ", ";
      for (auto counter : counters)
      {
        ss << counter.first << " = " << counter.second << ",";
      }
      ss << msg;
      return ss.str();
    }
  };
}