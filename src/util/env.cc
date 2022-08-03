// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <deque>
#include <set>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#if defined(LEVELDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif
#include "pebblesdb/env.h"
#include "pebblesdb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"

namespace leveldb {

Directory::~Directory() {
}
Directory::Directory(int fd, const std::string& directory_name)
    : fd_(fd), directory_name_(directory_name) {
  is_btrfs_ = false;
#ifdef OS_LINUX
  struct statfs buf;
  int ret = fstatfs(fd, &buf);
  is_btrfs_ = (ret == 0 && buf.f_type == static_cast<decltype(buf.f_type)>(
                                            BTRFS_SUPER_MAGIC));
#endif
}
Status Directory::Fsync(const IOOptions &options, IODebugContext *dbg, const DirFsyncOptions& dir_fsync_options)
 {
    Status s = Status::OK();
#ifndef OS_AIX
  if (is_btrfs_) {
    // skip dir fsync for new file creation, which is not needed for btrfs
    if (dir_fsync_options.reason == DirFsyncOptions::kNewFileSynced) {
      return s;
    }
    // skip dir fsync for renaming file, only need to sync new file
    if (dir_fsync_options.reason == DirFsyncOptions::kFileRenamed) {
      std::string new_name = dir_fsync_options.renamed_new_name;
      assert(!new_name.empty());
      int fd;
      do {
        // IOSTATS_TIMER_GUARD(open_nanos);
        fd = open(new_name.c_str(), O_RDONLY);
      } while (fd < 0 && errno == EINTR);
      if (fd < 0) {
        s = Status::IOError("While open renaming file");
      } else if (fsync(fd) < 0) {
        s = Status::IOError("While fsync renaming file");
      }
      if (close(fd) < 0) {
        s = Status::IOError("While closing file after fsync");
      }
      return s;
    }
    // fallback to dir-fsync for kDefault, kDirRenamed and kFileDeleted
  }

  // skip fsync/fcntl when fd_ == -1 since this file descriptor has been closed
  // in either the de-construction or the close function, data must have been
  // fsync-ed before de-construction and close is called
#ifdef HAVE_FULLFSYNC
  // btrfs is a Linux file system, while currently F_FULLFSYNC is available on
  // Mac OS.
  assert(!is_btrfs_);
  if (fd_ != -1 && ::fcntl(fd_, F_FULLFSYNC) < 0) {
    return Status::IOError("while fcntl(F_FULLFSYNC)", "a directory");
  }
#else   // HAVE_FULLFSYNC
  if (fd_ != -1 && fsync(fd_) == -1) {
    s = Status::IOError("While fsync", "a directory");
  }
#endif  // HAVE_FULLFSYNC
#endif  // OS_AIX
  return s;
}
Status Directory::FsyncWithDirOptions(
    const IOOptions &options, IODebugContext *dbg,
    const DirFsyncOptions &dir_fsync_options)
{
  return Fsync(options, dbg, dir_fsync_options);
}

Status Directory::Close() { 
  Status s = Status::OK();
  if (close(fd_) < 0) {
    s = Status::IOError("While closing directory ", directory_name_);
  } else {
    fd_ = -1;
  }
  return s;
}

size_t Directory::GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
  return 0;
}

Env::~Env() {
}

bool Env::FileExists(const std::string& fname) {
  return access(fname.c_str(), F_OK) == 0;
}

Status Env::GetChildren(const std::string& dir,
                            std::vector<std::string>* result) {
  result->clear();
  DIR* d = opendir(dir.c_str());
  if (d == NULL) {
    return Status::IOError(dir);
  }
  struct dirent* entry;
  while ((entry = readdir(d)) != NULL) {
    result->push_back(entry->d_name);
  }
  closedir(d);
  return Status::OK();
}

Status Env::DeleteFile(const std::string& fname) {
  Status result;
  if (unlink(fname.c_str()) != 0) {
    result = Status::IOError(fname);
  }
  return result;
}

Status Env::CreateDir(const std::string& name) {
  Status result;
  if (mkdir(name.c_str(), 0755) != 0) {
    result = Status::IOError(name);
  }
  return result;
}

Status Env::DeleteDir(const std::string& name) {
  Status result;
  if (rmdir(name.c_str()) != 0) {
    result = Status::IOError(name);
  }
  return result;
}

Status Env::GetFileSize(const std::string& fname, uint64_t* size) {
  Status s;
  struct stat sbuf;
  if (stat(fname.c_str(), &sbuf) != 0) {
    *size = 0;
    s = Status::IOError(fname);
  } else {
    *size = sbuf.st_size;
  }
  return s;
}

Status Env::RenameFile(const std::string& src, const std::string& target) {
  Status result;
  if (rename(src.c_str(), target.c_str()) != 0) {
    result = Status::IOError(src);
  }
  return result;
}

Status Env::CopyFile(const std::string& src, const std::string& target) {
  Status result;
  int fd1 = -1;
  int fd2 = -1;

  if (result.ok() && (fd1 = open(src.c_str(), O_RDONLY)) < 0) {
    result = Status::IOError(src);
  }
  if (result.ok() && (fd2 = open(target.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0) {
    result = Status::IOError(target);
  }

  ssize_t amt = 0;
  char buf[512];

  while (result.ok() && (amt = read(fd1, buf, 512)) > 0) {
    if (write(fd2, buf, amt) != amt) {
      result = Status::IOError(src);
    }
  }

  if (result.ok() && amt < 0) {
    result = Status::IOError(src);
  }

  if (fd1 >= 0 && close(fd1) < 0) {
    if (result.ok()) {
      result = Status::IOError(src);
    }
  }

  if (fd2 >= 0 && close(fd2) < 0) {
    if (result.ok()) {
      result = Status::IOError(target);
    }
  }

  return result;
}

Status Env::LinkFile(const std::string& src, const std::string& target) {
  Status result;
  if (link(src.c_str(), target.c_str()) != 0) {
    result = Status::IOError(src);
  }
  return result;
}
bool Env::DirExists(const std::string& dname) {
  struct stat statbuf;
  if (stat(dname.c_str(), &statbuf) == 0) {
    return S_ISDIR(statbuf.st_mode);
  }
  return false;  // stat() failed return false
}
// Creates directory if missing. Return Ok if it exists, or successful in
// Creating.
Status
Env::CreateDirIfMissing(const std::string &name)
{
  if (mkdir(name.c_str(), 0755) != 0) {
    if (errno != EEXIST) {
      return Status::IOError("While mkdir if missing");
    } else if (!DirExists(name)) {  // Check that name is actually a
                                    // directory.
      // Message is taken from mkdir
      return Status::IOError("`" + name +
                              "' exists but is not a directory");
    }
  }
  return Status::OK();
}
Status
Env::GetFileModificationTime(const std::string &fname,
                        uint64_t *file_mtime)
{
  struct stat s;
  if (stat(fname.c_str(), &s) != 0) {
    return Status::IOError("while stat a file for modification time");
  }
  *file_mtime = static_cast<uint64_t>(s.st_mtime);
  return Status::OK();
}
int Env::cloexec_flags(int flags, const EnvOptions* options) {
    // If the system supports opening the file with cloexec enabled,
    // do so, as this avoids a race condition if a db is opened around
    // the same time that a child process is forked
  #ifdef O_CLOEXEC
    if (options == nullptr || options->set_fd_cloexec) {
      flags |= O_CLOEXEC;
    }
  #else
    (void)options;
  #endif
    return flags;
}
Status
Env::IsDirectory(const std::string &path,
            bool *is_dir) 
{
  int fd = -1;
  int flags = cloexec_flags(O_RDONLY, nullptr);
  {
    fd = open(path.c_str(), flags);
  }
  if (fd < 0) {
    // return Error("While open for IsDirectory()", path, errno);
    return Status::IOError("While open for IsDirectory()");
  }
  Status io_s;
  struct stat sbuf;
  if (fstat(fd, &sbuf) < 0) {
    // io_s = Error("While doing stat for IsDirectory()", path, errno);
    io_s = Status::IOError("While doing stat for IsDirectory()");
  }
  close(fd);
  if (io_s.ok() && nullptr != is_dir) {
    *is_dir = S_ISDIR(sbuf.st_mode);
  }
  return io_s;
}
Status
Env::NewDirectory(const std::string &name,
                  Directory **result)
{
  result = nullptr;
  int fd;
  int flags = cloexec_flags(0, nullptr);
  {
    // IOSTATS_TIMER_GUARD(open_nanos);
    fd = open(name.c_str(), flags);
  }
  if (fd < 0) {
    return Status::IOError("While open directory");
  } else {
    *result = new Directory(fd, name);
  }
  return Status::OK();
}
std::string Env::invoke_strerror_r(char* (*strerror_r)(int, char*, size_t),
                                        int err, char* buf, size_t buflen) {
  // Using GNU strerror_r
  return strerror_r(err, buf, buflen);
}
std::string Env::errnoStr(int err) {
  char buf[1024];
  buf[0] = '\0';

  std::string result;

  // https://developer.apple.com/library/mac/documentation/Darwin/Reference/ManPages/man3/strerror_r.3.html
  // http://www.kernel.org/doc/man-pages/online/pages/man3/strerror.3.html
#if defined(_WIN32) && (defined(__MINGW32__) || defined(_MSC_VER))
  // mingw64 has no strerror_r, but Windows has strerror_s, which C11 added
  // as well. So maybe we should use this across all platforms (together
  // with strerrorlen_s). Note strerror_r and _s have swapped args.
  int r = strerror_s(buf, sizeof(buf), err);
  if (r != 0) {
    snprintf(buf, sizeof(buf),
            "Unknown error %d (strerror_r failed with error %d)", err, errno);
  }
  result.assign(buf);
#else
  // Using any strerror_r
  result.assign(invoke_strerror_r(strerror_r, err, buf, sizeof(buf)));
#endif

  return result;
}
Status
Env::GetAbsolutePath(const std::string &db_path,
                std::string *output_path)
{
  if (!db_path.empty() && db_path[0] == '/') {
  *output_path = db_path;
  return Status::OK();
  }

  char the_path[256];
  char* ret = getcwd(the_path, 256);
  if (ret == nullptr) {
    return Status::IOError(errnoStr(errno).c_str());
  }

  *output_path = ret;
  return Status::OK();
}
Status
Env::NumFileLinks(const std::string &fname,
              uint64_t *nr_links)
{
  struct stat s;
  if (stat(fname.c_str(), &s) != 0) {
    return Status::IOError("while stat a file for num file links");
  }
  *nr_links = static_cast<uint64_t>(s.st_nlink);
  return Status::OK();
}
Status
Env::AreFilesSame(const std::string& first, 
              const std::string& second,
              bool *res)
{
  struct stat statbuf[2];
  if (stat(first.c_str(), &statbuf[0]) != 0) {
    return Status::IOError("stat file");
  }
  if (stat(second.c_str(), &statbuf[1]) != 0) {
    return Status::IOError("stat file");
  }

  if (major(statbuf[0].st_dev) != major(statbuf[1].st_dev) ||
      minor(statbuf[0].st_dev) != minor(statbuf[1].st_dev) ||
      statbuf[0].st_ino != statbuf[1].st_ino) {
    *res = false;
  } else {
    *res = true;
  }
  return Status::OK();
}

SequentialFile::~SequentialFile() {
}

RandomAccessFile::~RandomAccessFile() {
}

WritableFile::~WritableFile() {
}

ConcurrentWritableFile::~ConcurrentWritableFile() {
}

Logger::~Logger() {
}

FileLock::~FileLock() {
}

void Log(Logger* info_log, const char* format, ...) {
  if (info_log != NULL) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(format, ap);
    va_end(ap);
  }
}

static Status DoWriteStringToFile(Env* env, const Slice& data,
                                  const std::string& fname,
                                  bool should_sync) {
  WritableFile* file;
  Status s = env->NewWritableFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data);
  if (s.ok() && should_sync) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  delete file;  // Will auto-close if we did not close above
  if (!s.ok()) {
    env->DeleteFile(fname);
  }
  return s;
}

Status WriteStringToFile(Env* env, const Slice& data,
                         const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, false);
}

Status WriteStringToFileSync(Env* env, const Slice& data,
                             const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, true);
}

Status ReadFileToString(Env* env, const std::string& fname, std::string* data) {
  data->clear();
  SequentialFile* file;
  Status s = env->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  static const int kBufferSize = 8192;
  char* space = new char[kBufferSize];
  while (true) {
    Slice fragment;
    s = file->Read(kBufferSize, &fragment, space);
    if (!s.ok()) {
      break;
    }
    data->append(fragment.data(), fragment.size());
    if (fragment.empty()) {
      break;
    }
  }
  delete[] space;
  delete file;
  return s;
}

EnvWrapper::~EnvWrapper() {
}

}  // namespace leveldb
