// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "db/zbd_zenfs.h"

namespace leveldb
{

  class ZoneExtent
  {
  public:
    uint64_t start_;
    uint64_t length_;
    Zone *zone_;

    explicit ZoneExtent(uint64_t start, uint64_t length, Zone *zone);
    Status DecodeFrom(Slice *input);
    void EncodeTo(std::string *output);
    void EncodeJson(std::ostream &json_stream);
  };

  class ZoneFile;

  /* Interface for persisting metadata for files */
  class MetadataWriter
  {
  public:
    virtual ~MetadataWriter();
    virtual Status Persist(ZoneFile *zoneFile) = 0;
  };

  class ZoneFile
  {
  private:
    const uint64_t NO_EXTENT = 0xffffffffffffffff;

    ZonedBlockDevice *zbd_;

    std::vector<ZoneExtent *> extents_;
    std::vector<std::string> linkfiles_;

    Zone *active_zone_;
    uint64_t extent_start_ = NO_EXTENT;
    uint64_t extent_filepos_ = 0;

    Env::WriteLifeTimeHint lifetime_;
    Env::IOType io_type_; /* Only used when writing */
    uint64_t file_size_;
    uint64_t file_id_;

    uint32_t nr_synced_extents_ = 0;
    bool open_for_wr_ = false;
    std::mutex open_for_wr_mtx_;

    time_t m_time_;
    bool is_sparse_ = false;
    bool is_deleted_ = false;

    MetadataWriter *metadata_writer_ = NULL;

    std::mutex writer_mtx_;
    std::atomic<int> readers_{0};

  public:
    static const int SPARSE_HEADER_SIZE = 8;

    explicit ZoneFile(ZonedBlockDevice *zbd, uint64_t file_id_,
                      MetadataWriter *metadata_writer);

    virtual ~ZoneFile();

    void AcquireWRLock();
    bool TryAcquireWRLock();
    void ReleaseWRLock();

    Status CloseWR();
    bool IsOpenForWR();

    Status PersistMetadata();

    Status Append(void *buffer, int data_size);
    Status BufferedAppend(char *data, uint32_t size);
    Status SparseAppend(char *data, uint32_t size);
    Status SetWriteLifeTimeHint(Env::WriteLifeTimeHint lifetime);
    void SetIOType(Env::IOType io_type);
    std::string GetFilename();
    time_t GetFileModificationTime();
    void SetFileModificationTime(time_t mt);
    uint64_t GetFileSize();
    void SetFileSize(uint64_t sz);
    void ClearExtents();

    uint32_t GetBlockSize() { return zbd_->GetBlockSize(); }
    ZonedBlockDevice *GetZbd() { return zbd_; }
    std::vector<ZoneExtent *> GetExtents() { return extents_; }
    Env::WriteLifeTimeHint GetWriteLifeTimeHint() { return lifetime_; }

    Status PositionedRead(uint64_t offset, size_t n, Slice *result,
                          char *scratch, bool direct);
    ZoneExtent *GetExtent(uint64_t file_offset, uint64_t *dev_offset);
    void PushExtent();
    Status AllocateNewZone();

    void EncodeTo(std::string *output, uint32_t extent_start);
    void EncodeUpdateTo(std::string *output)
    {
      EncodeTo(output, nr_synced_extents_);
    };
    void EncodeSnapshotTo(std::string *output) { EncodeTo(output, 0); };
    void EncodeJson(std::ostream &json_stream);
    void MetadataSynced() { nr_synced_extents_ = extents_.size(); };
    void MetadataUnsynced() { nr_synced_extents_ = 0; };

    Status MigrateData(uint64_t offset, uint32_t length, Zone *target_zone);

    Status DecodeFrom(Slice *input);
    Status MergeUpdate(ZoneFile* update, bool replace);

    uint64_t GetID() { return file_id_; }
    size_t GetUniqueId(char *id, size_t max_size);

    bool IsSparse() { return is_sparse_; };

    void SetSparse(bool is_sparse) { is_sparse_ = is_sparse; };
    uint64_t HasActiveExtent() { return extent_start_ != NO_EXTENT; };
    uint64_t GetExtentStart() { return extent_start_; };

    Status Recover();

    void ReplaceExtentList(std::vector<ZoneExtent *> new_list);
    void AddLinkName(const std::string &linkfile);
    Status RemoveLinkName(const std::string &linkfile);
    Status RenameLink(const std::string &src, const std::string &dest);
    uint32_t GetNrLinks() { return linkfiles_.size(); }
    const std::vector<std::string> &GetLinkFiles() const { return linkfiles_; }

  private:
    void ReleaseActiveZone();
    void SetActiveZone(Zone *zone);
    Status CloseActiveZone();

  public:
    ZenFSMetrics* GetZBDMetrics() { return zbd_->GetMetrics(); };
    Env::IOType GetIOType() const { return io_type_; };
    bool IsDeleted() const { return is_deleted_; };
    void SetDeleted() { is_deleted_ = true; };
    Status RecoverSparseExtents(uint64_t start, uint64_t end, Zone *zone);

  public:
    class ReadLock
    {
    public:
      ReadLock(ZoneFile *zfile) : zfile_(zfile)
      {
        zfile_->writer_mtx_.lock();
        zfile_->readers_++;
        zfile_->writer_mtx_.unlock();
      }
      ~ReadLock() { zfile_->readers_--; }

    private:
      ZoneFile *zfile_;
    };
    class WriteLock
    {
    public:
      WriteLock(ZoneFile *zfile) : zfile_(zfile)
      {
        zfile_->writer_mtx_.lock();
        while (zfile_->readers_ > 0)
        {
        }
      }
      ~WriteLock() { zfile_->writer_mtx_.unlock(); }

    private:
      ZoneFile *zfile_;
    };
  };

  class ZonedWritableFile : public WritableFile
  {
  public:
    explicit ZonedWritableFile(ZonedBlockDevice *zbd, bool buffered,
                               ZoneFile* zoneFile);
    virtual ~ZonedWritableFile();

    virtual Status Append(const Slice &data);
    virtual Status Append(const Slice &data,
                          const Env::DataVerificationInfo & /* verification_info */)
    {
      return Append(data);
    }
    virtual Status PositionedAppend(const Slice &data, uint64_t offset);
    virtual Status PositionedAppend(
        const Slice &data, uint64_t offset,
        const Env::DataVerificationInfo & /* verification_info */)
    {
      return PositionedAppend(data, offset);
    }
    virtual Status Truncate(uint64_t size);
    virtual Status Close();
    virtual Status Flush();
    virtual Status Sync();
    virtual Status RangeSync(uint64_t offset, uint64_t nbytes);
    virtual Status Fsync();

    bool use_direct_io() const { return !buffered; }
    bool IsSyncThreadSafe() const { return true; };
    size_t GetRequiredBufferAlignment() const
    {
      return zoneFile_->GetBlockSize();
    }
      void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint);
  virtual Env::WriteLifeTimeHint GetWriteLifeTimeHint() {
    return zoneFile_->GetWriteLifeTimeHint();
  }

  private:
    Status BufferedWrite(const Slice &data);
    Status FlushBuffer();
    Status DataSync();
    Status CloseInternal();

    bool buffered;
    char *sparse_buffer;
    char *buffer;
    size_t buffer_sz;
    uint32_t block_sz;
    uint32_t buffer_pos;
    uint64_t wp;
    int write_temp;
    bool open;

    ZoneFile* zoneFile_;
    MetadataWriter *metadata_writer_;

    std::mutex buffer_mtx_;
  };

  class ZonedSequentialFile : public SequentialFile
  {
  private:
    ZoneFile* zoneFile_;
    uint64_t rp;
    bool direct_;

  public:
    explicit ZonedSequentialFile(ZoneFile* zoneFile)
        : zoneFile_(zoneFile),
          rp(0) {}

    Status Read(size_t n, Slice *result,
                char *scratch);
    Status PositionedRead(uint64_t offset, size_t n,
                          Slice *result, char *scratch);
    Status Skip(uint64_t n);

    bool use_direct_io() const { return direct_; };

    size_t GetRequiredBufferAlignment() const
    {
      return zoneFile_->GetBlockSize();
    }

    Status InvalidateCache(size_t /*offset*/, size_t /*length*/)
    {
      return Status::OK();
    }
  };

  class ZonedRandomAccessFile : public RandomAccessFile
  {
  private:
    ZoneFile* zoneFile_;
    bool direct_;

  public:
    explicit ZonedRandomAccessFile(ZoneFile* zoneFile)
        : zoneFile_(zoneFile) {}

    Status Read(uint64_t offset, size_t n,
                Slice *result, char *scratch) const;

    Status Prefetch(uint64_t /*offset*/, size_t /*n*/)
    {
      return Status::OK();
    }

    bool use_direct_io() const { return direct_; }

    size_t GetRequiredBufferAlignment() const
    {
      return zoneFile_->GetBlockSize();
    }

    Status InvalidateCache(size_t /*offset*/, size_t /*length*/)
    {
      return Status::OK();
    }

    size_t GetUniqueId(char *id, size_t max_size) const;
  };

} // namespace leveldb
