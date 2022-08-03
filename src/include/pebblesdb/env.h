// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An Env is an interface used by the leveldb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_ENV_H_
#define STORAGE_LEVELDB_INCLUDE_ENV_H_

#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <ostream>
#include <algorithm>
#include <fstream>
#include <stdarg.h>
#include <stdint.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include <sys/sysmacros.h>
#include <sys/time.h>

#include "db/io_config.h"
#include "pebblesdb/status.h"

namespace leveldb
{
  enum InfoLogLevel : unsigned char
  {
    DEBUG_LEVEL = 0,
    INFO_LEVEL,
    WARN_LEVEL,
    ERROR_LEVEL,
    FATAL_LEVEL,
    HEADER_LEVEL,
    NUM_INFO_LOG_LEVELS,
  };

  class FileLock;
  class Logger;
  class RandomAccessFile;
  class SequentialFile;
  class Slice;
  class WritableFile;
  class ConcurrentWritableFile;

  // Directory object represents collection of files and implements
  // filesystem operations that can be executed on directories.
  class Directory
  {
  public:
    explicit Directory(int fd, const std::string& directory_name);
    virtual ~Directory();
    // Fsync directory. Can be called concurrently from multiple threads.
    virtual Status Fsync(const IOOptions &options, IODebugContext *dbg, const DirFsyncOptions& dir_fsync_options);

    // FsyncWithDirOptions after renaming a file. Depends on the filesystem, it
    // may fsync directory or just the renaming file (e.g. btrfs). By default, it
    // just calls directory fsync.
    virtual Status FsyncWithDirOptions(
        const IOOptions &options, IODebugContext *dbg,
        const DirFsyncOptions &dir_fsync_options);

    // Close directory
    virtual Status Close();

    virtual size_t GetUniqueId(char * /*id*/, size_t /*max_size*/) const;

    // If you're adding methods here, remember to add them to
    // DirectoryWrapper too.
  private:
    int fd_;
    bool is_btrfs_;
    const std::string directory_name_;
  };

  #if !defined(BTRFS_SUPER_MAGIC)
  // The magic number for BTRFS is fixed, if it's not defined, define it here
  #define BTRFS_SUPER_MAGIC 0x9123683E
  #endif

  // A data structure brings the data verification information, which is
  // used together with data being written to a file.
  struct DataVerificationInfo
  {
    // checksum of the data being written.
    Slice checksum;
  };

  class Env
  {
  public:
    Env()
    {
    }

    // Priority for requesting bytes in rate limiter scheduler
    enum IOPriority : u_int8_t
    {
      IO_LOW = 0,
      IO_MID = 1,
      IO_HIGH = 2,
      IO_USER = 3,
      IO_TOTAL = 4
    };

    // Options while opening a file to read/write
    struct EnvOptions
    {
      // Construct with default Options
      EnvOptions();

      // Construct from Options
      // explicit EnvOptions(const DBOptions &options);

      // If true, then use mmap to read data.
      // Not recommended for 32-bit OS.
      bool use_mmap_reads = false;

      // If true, then use mmap to write data
      bool use_mmap_writes = true;

      // If true, then use O_DIRECT for reading data
      bool use_direct_reads = false;

      // If true, then use O_DIRECT for writing data
      bool use_direct_writes = false;

      // If false, fallocate() calls are bypassed
      bool allow_fallocate = true;

      // If true, set the FD_CLOEXEC on open fd.
      bool set_fd_cloexec = true;

      // Allows OS to incrementally sync files to disk while they are being
      // written, in the background. Issue one request for every bytes_per_sync
      // written. 0 turns it off.
      // Default: 0
      uint64_t bytes_per_sync = 0;

      // When true, guarantees the file has at most `bytes_per_sync` bytes submitted
      // for writeback at any given time.
      //
      //  - If `sync_file_range` is supported it achieves this by waiting for any
      //    prior `sync_file_range`s to finish before proceeding. In this way,
      //    processing (compression, etc.) can proceed uninhibited in the gap
      //    between `sync_file_range`s, and we block only when I/O falls behind.
      //  - Otherwise the `WritableFile::Sync` method is used. Note this mechanism
      //    always blocks, thus preventing the interleaving of I/O and processing.
      //
      // Note: Enabling this option does not provide any additional persistence
      // guarantees, as it may use `sync_file_range`, which does not write out
      // metadata.
      //
      // Default: false
      bool strict_bytes_per_sync = false;

      // If true, we will preallocate the file with FALLOC_FL_KEEP_SIZE flag, which
      // means that file size won't change as part of preallocation.
      // If false, preallocation will also change the file size. This option will
      // improve the performance in workloads where you sync the data on every
      // write. By default, we set it to true for MANIFEST writes and false for
      // WAL writes
      bool fallocate_with_keep_size = true;

      // See DBOptions doc
      size_t compaction_readahead_size = 0;

      // See DBOptions doc
      size_t random_access_max_buffer_size = 0;

      // See DBOptions doc
      size_t writable_file_max_buffer_size = 1024 * 1024;

      // If not nullptr, write rate limiting is enabled for flush and compaction
      // RateLimiter *rate_limiter = nullptr;
    };

    // Temperature of a file. Used to pass to FileSystem for a different
    // placement and/or coding.
    // Reserve some numbers in the middle, in case we need to insert new tier
    // there.
    enum class Temperature : uint8_t
    {
      kUnknown = 0,
      kHot = 0x04,
      kWarm = 0x08,
      kCold = 0x0C,
      kLastTemperature,
    };

    // Types of checksums to use for checking integrity of logical blocks within
    // files. All checksums currently use 32 bits of checking power (1 in 4B
    // chance of failing to detect random corruption).
    enum ChecksumType : char
    {
      kNoChecksum = 0x0,
      kCRC32c = 0x1,
      kxxHash = 0x2,
      kxxHash64 = 0x3,
      kXXH3 = 0x4, // Supported since RocksDB 6.27
    };

    // File scope options that control how a file is opened/created and accessed
    // while its open. We may add more options here in the future such as
    // redundancy level, media to use etc.
    struct FileOptions : EnvOptions
    {
      // Embedded IOOptions to control the parameters for any IOs that need
      // to be issued for the file open/creation
      IOOptions io_options;

      // EXPERIMENTAL
      // The feature is in development and is subject to change.
      // When creating a new file, set the temperature of the file so that
      // underlying file systems can put it with appropriate storage media and/or
      // coding.
      Temperature temperature = Temperature::kUnknown;

      // The checksum type that is used to calculate the checksum value for
      // handoff during file writes.
      ChecksumType handoff_checksum_type;

      FileOptions() : EnvOptions(), handoff_checksum_type(ChecksumType::kCRC32c) {}

      // FileOptions(const DBOptions& opts)
      //     : EnvOptions(opts), handoff_checksum_type(ChecksumType::kCRC32c) {}

      FileOptions(const EnvOptions &opts)
          : EnvOptions(opts), handoff_checksum_type(ChecksumType::kCRC32c) {}

      FileOptions(const FileOptions &opts)
          : EnvOptions(opts),
            io_options(opts.io_options),
            temperature(opts.temperature),
            handoff_checksum_type(opts.handoff_checksum_type) {}

      FileOptions &operator=(const FileOptions &) = default;
    };

    virtual ~Env();

    // Return a default environment suitable for the current operating
    // system.  Sophisticated users may wish to provide their own Env
    // implementation instead of relying on this default environment.
    //
    // The result of Default() belongs to leveldb and must never be deleted.
    static Env *
    Default();

    // Deprecated. Will be removed in a major release. Derived classes
    // should implement this method.
    // const char *Name() const { return ""; }

    // Create a brand new sequentially-readable file with the specified name.
    // On success, stores a pointer to the new file in *result and returns OK.
    // On failure stores NULL in *result and returns non-OK.  If the file does
    // not exist, returns a non-OK status.
    //
    // The returned file will only be accessed by one thread at a time.
    virtual Status
    NewSequentialFile(const std::string &fname,
                      SequentialFile **result) = 0;
    // virtual Status
    // NewSequentialFile(const std::string &fname,
    //                   const FileOptions &file_opts,
    //                   std::unique_ptr<SequentialFile> *result,
    //                   IODebugContext *dbg);

    // Create a brand new random access read-only file with the
    // specified name.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores NULL in *result and
    // returns non-OK.  If the file does not exist, returns a non-OK
    // status.
    //
    // The returned file may be concurrently accessed by multiple threads.
    virtual Status
    NewRandomAccessFile(const std::string &fname,
                        RandomAccessFile **result) = 0;
    // virtual Status
    // NewRandomAccessFile(const std::string &fname,
    //                     const FileOptions &file_opts,
    //                     std::unique_ptr<RandomAccessFile> *result,
    //                     IODebugContext *dbg);

    // Create an object that writes to a new file with the specified
    // name.  Deletes any existing file with the same name and creates a
    // new file.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores NULL in *result and
    // returns non-OK.
    //
    // The returned file will only be accessed by one thread at a time.
    virtual Status
    NewWritableFile(const std::string &fname,
                    WritableFile **result) = 0;

    virtual Status
    NewConcurrentWritableFile(const std::string &fname,
                              ConcurrentWritableFile **result) = 0;

    // Returns true iff the named file exists.
    virtual bool
    FileExists(const std::string &fname) = 0;

    // Store in *result the names of the children of the specified directory.
    // The names are relative to "dir".
    // Original contents of *results are dropped.
    virtual Status
    GetChildren(const std::string &dir,
                std::vector<std::string> *result) = 0;

    // Delete the named file.
    virtual Status
    DeleteFile(const std::string &fname) = 0;

    // Create the specified directory.
    virtual Status
    CreateDir(const std::string &dirname) = 0;

    // Delete the specified directory.
    virtual Status
    DeleteDir(const std::string &dirname) = 0;

    // Store the size of fname in *file_size.
    virtual Status
    GetFileSize(const std::string &fname, uint64_t *file_size) = 0;

    // Rename file src to target.
    virtual Status
    RenameFile(const std::string &src,
               const std::string &target) = 0;

    // Copy file src to target.
    virtual Status
    CopyFile(const std::string &src,
             const std::string &target) = 0;

    // Link file src to target.
    virtual Status
    LinkFile(const std::string &src,
             const std::string &target) = 0;

    // Lock the specified file.  Used to prevent concurrent access to
    // the same db by multiple processes.  On failure, stores NULL in
    // *lock and returns non-OK.
    //
    // On success, stores a pointer to the object that represents the
    // acquired lock in *lock and returns OK.  The caller should call
    // UnlockFile(*lock) to release the lock.  If the process exits,
    // the lock will be automatically released.
    //
    // If somebody else already holds the lock, finishes immediately
    // with a failure.  I.e., this call does not wait for existing locks
    // to go away.
    //
    // May create the named file if it does not already exist.
    virtual Status
    LockFile(const std::string &fname, FileLock **lock) = 0;

    // Release the lock acquired by a previous successful call to LockFile.
    // REQUIRES: lock was returned by a successful LockFile() call
    // REQUIRES: lock has not already been unlocked.
    virtual Status
    UnlockFile(FileLock *lock) = 0;

    virtual bool DirExists(const std::string& dname);
    // Creates directory if missing. Return Ok if it exists, or successful in
    // Creating.
    virtual Status
    CreateDirIfMissing(const std::string &name);

    // Store the last modification time of fname in *file_mtime.
    virtual Status
    GetFileModificationTime(const std::string &fname,
                            uint64_t *file_mtime);

    // Arrange to run "(*function)(arg)" once in a background thread.
    //
    // "function" may run in an unspecified thread.  Multiple functions
    // added to the same Env may run concurrently in different threads.
    // I.e., the caller may not assume that background work items are
    // serialized.
    virtual void
    Schedule(
        void (*function)(void *arg),
        void *arg) = 0;

    // Start a new thread, invoking "function(arg)" within the new thread.
    // When "function(arg)" returns, the thread will be destroyed.
    virtual void
    StartThread(void (*function)(void *arg), void *arg) = 0;

    // Start a new thread, invoking "function(arg)" within the new thread.
    // When "function(arg)" returns, the thread will be destroyed.
    virtual pthread_t
    StartThreadAndReturnThreadId(void (*function)(void *arg), void *arg) = 0;

    // Wait for the thread th to complete
    // Store the exit status of the thread in return_status
    virtual void
    WaitForThread(unsigned long int th, void **return_status) = 0;

    // *path is set to a temporary directory that can be used for testing. It may
    // or many not have just been created. The directory may or may not differ
    // between runs of the same process, but subsequent calls will return the
    // same directory.
    virtual Status
    GetTestDirectory(std::string *path) = 0;

    // Create and return a log file for storing informational messages.
    virtual Status
    NewLogger(const std::string &fname, Logger **result) = 0;

    // Returns the number of micro-seconds since some fixed point in time. Only
    // useful for computing deltas of time.
    virtual uint64_t
    NowMicros() = 0;

    // Sleep/delay the thread for the perscribed number of micro-seconds.
    virtual void
    SleepForMicroseconds(int micros) = 0;

    virtual pthread_t
    GetThreadId() = 0;

    int cloexec_flags(int flags, const EnvOptions* options);
    // Check whether the specified path is a directory
    virtual Status
    IsDirectory(const std::string &path,
                bool *is_dir);

    virtual Status
    NewDirectory(const std::string &name,
                 Directory **result);

    std::string invoke_strerror_r(char* (*strerror_r)(int, char*, size_t),
                                        int err, char* buf, size_t buflen);
    std::string errnoStr(int err);
    // Get full directory name for this db.
    virtual Status
    GetAbsolutePath(const std::string &db_path,
                    std::string *output_path);

    virtual Status
    NumFileLinks(const std::string &fname,
                 uint64_t *nr_links);

    virtual Status
    AreFilesSame(const std::string& first, 
                 const std::string& second,
                 bool *res);

    bool GenerateRfcUuid(std::string* output) {
      output->clear();
      std::ifstream f("/proc/sys/kernel/random/uuid");
      std::getline(f, /*&*/ *output);
      if (output->size() == 36) {
        return true;
      } else {
        output->clear();
        return false;
      }
  }

    std::string GenerateUniqueId() {
      std::string result;
      GenerateRfcUuid(&result);
      return result;
    }
  private:
    // No copying allowed
    Env(const Env &);
    void
    operator=(const Env &);
  };

  // A file abstraction for reading sequentially through a file
  class SequentialFile
  {
  public:
    SequentialFile()
    {
    }

    virtual ~SequentialFile();

    // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
    // written by this routine.  Sets "*result" to the data that was
    // read (including if fewer than "n" bytes were successfully read).
    // May set "*result" to point at data in "scratch[0..n-1]", so
    // "scratch[0..n-1]" must be live when "*result" is used.
    // If an error was encountered, returns a non-OK status.
    //
    // REQUIRES: External synchronization
    virtual Status
    Read(size_t n, Slice *result, char *scratch) = 0;

    // Skip "n" bytes from the file. This is guaranteed to be no
    // slower that reading the same data, but may be faster.
    //
    // If end of file is reached, skipping will stop at the end of the
    // file, and Skip will return OK.
    //
    // REQUIRES: External synchronization
    virtual Status
    Skip(uint64_t n) = 0;

  private:
    // No copying allowed
    SequentialFile(const SequentialFile &);
    void
    operator=(const SequentialFile &);
  };

  // A file abstraction for randomly reading the contents of a file.
  class RandomAccessFile
  {
  public:
    RandomAccessFile()
    {
    }

    virtual ~RandomAccessFile();

    // Read up to "n" bytes from the file starting at "offset".
    // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
    // to the data that was read (including if fewer than "n" bytes were
    // successfully read).  May set "*result" to point at data in
    // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
    // "*result" is used.  If an error was encountered, returns a non-OK
    // status.
    //
    // Safe for concurrent use by multiple threads.
    virtual Status
    Read(uint64_t offset, size_t n, Slice *result,
         char *scratch) const = 0;

  private:
    // No copying allowed
    RandomAccessFile(const RandomAccessFile &);
    void
    operator=(const RandomAccessFile &);
  };

  // A file abstraction for sequential writing.  The implementation
  // must provide buffering since callers may append small fragments
  // at a time to the file.
  class WritableFile
  {
  public:
    WritableFile()
    {
    }

    virtual ~WritableFile();

    // REQUIRES:  external synchronization
    virtual Status
    Append(const Slice &data) = 0;
    virtual Status
    Close() = 0;
    virtual Status
    Flush() = 0;
    virtual Status
    Sync() = 0;
    virtual Status
    Fsync() {return Sync();}
    virtual void 
    SetWriteLifeTimeHint(WriteLifeTimeHint hint) {
      write_hint_ = hint;
    }

  protected:
    WriteLifeTimeHint write_hint_;

  private:
    // No copying allowed
    WritableFile(const WritableFile &);
    WritableFile &
    operator=(const WritableFile &);
  };

  // A file abstraction for concurrent nearly-sequential writing.  The
  // implementation should provide buffering, and must permit multiple callers to
  // call the public methods simultaneously.
  class ConcurrentWritableFile : public WritableFile
  {
  public:
    ConcurrentWritableFile()
    {
    }

    virtual ~ConcurrentWritableFile();

    // Allows concurrent writers
    // REQUIRES:  The range of data falling in [offset, offset + data.size()) must
    // only be written once.
    virtual Status
    WriteAt(uint64_t offset, const Slice &data) = 0;

  private:
    // No copying allowed
    ConcurrentWritableFile(const ConcurrentWritableFile &);
    ConcurrentWritableFile &
    operator=(const ConcurrentWritableFile &);
  };

  // An interface for writing log messages.
  class Logger
  {
  public:
    Logger()
    {
    }

    virtual ~Logger();

    // Write an entry to the log file with the specified format.
    virtual void
    Logv(const char *format, va_list ap) = 0;

  private:
    // No copying allowed
    Logger(const Logger &);
    void
    operator=(const Logger &);
  };

  // Identifies a locked file.
  class FileLock
  {
  public:
    FileLock()
    {
    }

    virtual ~FileLock();

  private:
    // No copying allowed
    FileLock(const FileLock &);
    void
    operator=(const FileLock &);
  };

  // Log the specified data to *info_log if info_log is non-NULL.
  extern void
  Log(Logger *info_log, const char *format, ...)
#if defined(__GNUC__) || defined(__clang__)
      __attribute__((__format__(__printf__, 2, 3)))
#endif
      ;

  // A utility routine: write "data" to the named file.
  extern Status
  WriteStringToFile(Env *env, const Slice &data,
                    const std::string &fname);

  // A utility routine: read contents of named file into *data
  extern Status
  ReadFileToString(Env *env, const std::string &fname,
                   std::string *data);

  // An implementation of Env that forwards all calls to another Env.
  // May be useful to clients who wish to override just part of the
  // functionality of another Env.
  class EnvWrapper : public Env
  {
  public:
    EnvWrapper();
    // Initialize an EnvWrapper that delegates all calls to *t
    explicit EnvWrapper(Env *t) : target_(t)
    {
    }

    virtual ~EnvWrapper();

    // Return the target to which this Env forwards all calls
    Env *
    target() const
    {
      return target_;
    }

    // The following text is boilerplate that forwards all methods to target()
    Status
    NewSequentialFile(const std::string &f, SequentialFile **r)
    {
      return target_->NewSequentialFile(f, r);
    }
    // Status
    // NewSequentialFile(const std::string &fname,
    //                   std::unique_ptr<SequentialFile> *result)
    // {
    //   SequentialFile *r = result->get();
    //   return target_->NewSequentialFile(fname, &r);
    // }

    Status
    NewRandomAccessFile(const std::string &f, RandomAccessFile **r)
    {
      return target_->NewRandomAccessFile(f, r);
    }
    // Status
    // NewRandomAccessFile(const std::string &fname,
    //                     std::unique_ptr<RandomAccessFile> *result)
    // {
    //   RandomAccessFile *r = result->get();
    //   return target_->NewRandomAccessFile(fname, &r);
    // }

    Status
    NewWritableFile(const std::string &f, WritableFile **r)
    {
      return target_->NewWritableFile(f, r);
    }
    // Status
    // NewWritableFile(const std::string &fname,
    //                 std::unique_ptr<WritableFile> *result)
    // {
    //   WritableFile *r = result->get();
    //   return target_->NewWritableFile(fname, &r);
    // }

    Status
    NewConcurrentWritableFile(const std::string &f, ConcurrentWritableFile **r)
    {
      return target_->NewConcurrentWritableFile(f, r);
    }

    bool
    FileExists(const std::string &f)
    {
      return target_->FileExists(f);
    }
    // virtual Status
    // FileExists(const std::string &fname,
    //            const IOOptions &options,
    //            IODebugContext *dbg)
    // {
    //   return target_->FileExists(fname, options, dbg);
    // }

    Status
    GetChildren(const std::string &dir, std::vector<std::string> *r)
    {
      return target_->GetChildren(dir, r);
    }
    // Status
    // GetChildren(const std::string &dir,
    //             const IOOptions &options,
    //             std::vector<std::string> *result,
    //             IODebugContext *dbg)
    // {
    //   return target_->GetChildren(dir, options, result, dbg);
    // }

    Status
    DeleteFile(const std::string &f)
    {
      return target_->DeleteFile(f);
    }
    // Status
    // DeleteFile(const std::string &f, const IOOptions &options,
    //            IODebugContext *dbg) override
    // {
    //   return target_->DeleteFile(f, options, dbg);
    // }

    Status
    CreateDir(const std::string &d)
    {
      return target_->CreateDir(d);
    }
    // Status
    // CreateDir(const std::string &d,
    //           const IOOptions &options,
    //           IODebugContext *dbg)
    // {
    //   return target_->CreateDir(d, options, dbg);
    // }

    Status
    DeleteDir(const std::string &d)
    {
      return target_->DeleteDir(d);
    }
    // Status
    // DeleteDir(const std::string &d,
    //           const IOOptions &options,
    //           IODebugContext *dbg)
    // {
    //   return target_->DeleteDir(d, options, dbg);
    // }

    Status
    GetFileSize(const std::string &f, uint64_t *s)
    {
      return target_->GetFileSize(f, s);
    }
    // Status
    // GetFileSize(const std::string &f,
    //             const IOOptions &options,
    //             uint64_t *size,
    //             IODebugContext *dbg)
    // {
    //   return target_->GetFileSize(f, options, size, dbg);
    // }

    Status
    RenameFile(const std::string &s, const std::string &t)
    {
      return target_->RenameFile(s, t);
    }
    // Status
    // RenameFile(const std::string &f,
    //            const std::string &t,
    //            const IOOptions &options,
    //            IODebugContext *dbg)
    // {
    //   return target_->RenameFile(f, t, options, dbg);
    // }

    Status
    CopyFile(const std::string &s, const std::string &t)
    {
      return target_->CopyFile(s, t);
    }

    Status
    LinkFile(const std::string &s, const std::string &t)
    {
      return target_->LinkFile(s, t);
    }

    Status
    LockFile(const std::string &f, FileLock **l)
    {
      return target_->LockFile(f, l);
    }

    Status
    CreateDirIfMissing(const std::string &d)
    {
      return target_->CreateDirIfMissing(d);
    }

    Status
    GetFileModificationTime(const std::string &fname,
                            uint64_t *file_mtime)
    {
      return target_->GetFileModificationTime(fname, file_mtime);
    }
    // Status
    // GetFileModificationTime(const std::string &fname,
    //                         const IOOptions &options,
    //                         uint64_t *mtime,
    //                         IODebugContext *dbg)
    // {
    //   return target_->GetFileModificationTime(fname, options, mtime, dbg);
    // }

    Status
    UnlockFile(FileLock *l)
    {
      return target_->UnlockFile(l);
    }
    // Status
    // UnlockFile(FileLock *l,
    //            const IOOptions &options,
    //            IODebugContext *dbg)
    // {
    //   return target_->UnlockFile(l, options, dbg);
    // }

    void
    Schedule(void (*f)(void *), void *a)
    {
      return target_->Schedule(f, a);
    }

    void
    StartThread(void (*f)(void *), void *a)
    {
      return target_->StartThread(f, a);
    }

    pthread_t
    StartThreadAndReturnThreadId(void (*f)(void *), void *a)
    {
      return target_->StartThreadAndReturnThreadId(f, a);
    }

    void
    WaitForThread(unsigned long int th, void **return_status)
    {
      return target_->WaitForThread(th, return_status);
    }

    virtual Status
    GetTestDirectory(std::string *path)
    {
      return target_->GetTestDirectory(path);
    }
    // virtual Status
    // GetTestDirectory(const IOOptions &options,
    //                  std::string *path,
    //                  IODebugContext *dbg)
    // {
    //   return target_->GetTestDirectory(options, path, dbg);
    // }

    virtual Status
    NewLogger(const std::string &fname, Logger **result)
    {
      return target_->NewLogger(fname, result);
    }

    uint64_t
    NowMicros()
    {
      return target_->NowMicros();
    }

    void
    SleepForMicroseconds(int micros)
    {
      target_->SleepForMicroseconds(micros);
    }

    pthread_t
    GetThreadId()
    {
      target_->GetThreadId();
    }

    Status IsDirectory(const std::string &path,
                       bool *is_dir)
    {
      return target_->IsDirectory(path, is_dir);
    }

    Status
    NewDirectory(const std::string &name,
                 Directory **result)
    {
      return target_->NewDirectory(name, result);
    }

    Status
    GetAbsolutePath(const std::string &db_path,
                    std::string *output_path)
    {
      return target_->GetAbsolutePath(db_path, output_path);
    }

    Status
    NumFileLinks(const std::string &fname,
                 uint64_t *nr_links)
    {
      return target_->NumFileLinks(fname, nr_links);
    }

    Status AreFilesSame(const std::string &fname,
                        const std::string &link,
                        bool *res)
    {
      return target_->AreFilesSame(fname, link, res);
    }

  private:
    EnvWrapper(EnvWrapper &);
    EnvWrapper &
    operator=(EnvWrapper &);
    Env *target_;
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_INCLUDE_ENV_H_
