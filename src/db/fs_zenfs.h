// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if __cplusplus < 201703L
#include "db/filesystem_utility.h"
namespace fs = filesystem_utility;
#else
#include <filesystem>
namespace fs = std::filesystem;
#endif

#include <memory>
#include <map>

#include "db/io_zenfs.h"
#include "db/metric.h"
#include "db/zbd_zenfs.h"
#include "db/version.h"
#include "pebblesdb/env.h"
#include "pebblesdb/status.h"
#include "db/snapshot.h"

namespace leveldb
{
    bool GenerateRfcUuid(std::string* output);
    std::string GenerateUniqueId();

    class ZoneSnapshot;
    class ZoneFileSnapshot;
    class ZenFSSnapshot;
    class ZenFSSnapshotOptions;
    class ZoneExtentSnapshot;

    class Superblock
    {
        uint32_t magic_ = 0;
        char uuid_[37] = {0};
        uint32_t sequence_ = 0;
        uint32_t superblock_version_ = 0;
        uint32_t flags_ = 0;
        uint32_t block_size_ = 0; /* in bytes */
        uint32_t zone_size_ = 0;  /* in blocks */
        uint32_t nr_zones_ = 0;
        char aux_fs_path_[256] = {0};
        uint32_t finish_treshold_ = 0;
        char zenfs_version_[64]{0};
        char reserved_[123] = {0};

    public:
        const uint32_t MAGIC = 0x5a454e46; /* ZENF */
        const uint32_t ENCODED_SIZE = 512;
        const uint32_t CURRENT_SUPERBLOCK_VERSION = 2;
        const uint32_t DEFAULT_FLAGS = 0;

        Superblock() {}

        /* Create a superblock for a filesystem covering the entire zoned block device
         */
        Superblock(ZonedBlockDevice *zbd, std::string aux_fs_path = "",
                   uint32_t finish_threshold = 0)
        {
            std::string uuid = leveldb::GenerateUniqueId();
            int uuid_len =
                std::min(uuid.length(),
                         sizeof(uuid_) - 1); /* make sure uuid is nullterminated */
            memcpy((void *)uuid_, uuid.c_str(), uuid_len);
            magic_ = MAGIC;
            superblock_version_ = CURRENT_SUPERBLOCK_VERSION;
            flags_ = DEFAULT_FLAGS;
            finish_treshold_ = finish_threshold;

            block_size_ = zbd->GetBlockSize();
            zone_size_ = zbd->GetZoneSize() / block_size_;
            nr_zones_ = zbd->GetNrZones();

            strncpy(aux_fs_path_, aux_fs_path.c_str(), sizeof(aux_fs_path_) - 1);

            std::string zenfs_version = ZENFS_VERSION;
            strncpy(zenfs_version_, zenfs_version.c_str(), sizeof(zenfs_version_) - 1);
        }

        Status DecodeFrom(Slice *input);
        void EncodeTo(std::string *output);
        Status CompatibleWith(ZonedBlockDevice *zbd);

        void GetReport(std::string *reportString);

        uint32_t GetSeq() { return sequence_; }
        std::string GetAuxFsPath() { return std::string(aux_fs_path_); }
        uint32_t GetFinishTreshold() { return finish_treshold_; }
        std::string GetUUID() { return std::string(uuid_); }
    };

    class ZenMetaLog
    {
        uint64_t read_pos_;
        Zone *zone_;
        ZonedBlockDevice *zbd_;
        size_t bs_;

        /* Every meta log record is prefixed with a CRC(32 bits) and record length (32
         * bits) */
        const size_t zMetaHeaderSize = sizeof(uint32_t) * 2;

    public:
        ZenMetaLog(ZonedBlockDevice *zbd, Zone *zone)
        {
            assert(zone->IsBusy());
            zbd_ = zbd;
            zone_ = zone;
            bs_ = zbd_->GetBlockSize();
            read_pos_ = zone->start_;
        }

        virtual ~ZenMetaLog()
        {
            // TODO: report async error status
            bool ok = zone_->Release();
            assert(ok);
            (void)ok;
        }

        Status AddRecord(const Slice &slice);
        Status ReadRecord(Slice *record, std::string *scratch);

        Zone *GetZone() { return zone_; };

    private:
        Status Read(Slice *slice);
    };

    class ZenFS : public EnvWrapper
    {
        ZonedBlockDevice *zbd_;
        std::map<std::string, ZoneFile*> files_;
        std::mutex files_mtx_;
        Logger *logger_;
        std::atomic<uint64_t> next_file_id_;

        Zone *cur_meta_zone_ = nullptr;
        std::unique_ptr<ZenMetaLog> meta_log_;
        std::mutex metadata_sync_mtx_;
        std::unique_ptr<Superblock> superblock_;

        Logger *GetLogger() { return logger_; }

        struct ZenFSMetadataWriter : public MetadataWriter
        {
            ZenFS *zenFS;
            Status Persist(ZoneFile *zoneFile)
            {
                return zenFS->SyncFileMetadata(zoneFile);
            }
        };

        ZenFSMetadataWriter metadata_writer_;

        enum ZenFSTag : uint32_t
        {
            kCompleteFilesSnapshot = 1,
            kFileUpdate = 2,
            kFileDeletion = 3,
            kEndRecord = 4,
            kFileReplace = 5,
        };

        void LogFiles();
        void ClearFiles();
        std::string FormatPathLexically(fs::path filepath);
        Status WriteSnapshotLocked(ZenMetaLog *meta_log);
        Status WriteEndRecord(ZenMetaLog *meta_log);
        Status RollMetaZoneLocked();
        Status PersistSnapshot(ZenMetaLog *meta_writer);
        Status PersistRecord(std::string record);
        Status SyncFileExtents(ZoneFile *zoneFile,
                                 std::vector<ZoneExtent *> new_extents);
        /* Must hold files_mtx_ */
        // Status SyncFileMetadataNoLock(ZoneFile *zoneFile, bool replace = false);
        /* Must hold files_mtx_ */
        Status SyncFileMetadataNoLock(ZoneFile* zoneFile,
                                     bool replace = false)
        {
            return SyncFileMetadataNoLock(zoneFile, replace);
        }
        // Status SyncFileMetadata(ZoneFile *zoneFile, bool replace = false);
        Status SyncFileMetadata(ZoneFile* zoneFile,
                                  bool replace = false)
        {
            return SyncFileMetadata(zoneFile, replace);
        }

        void EncodeSnapshotTo(std::string *output);
        void EncodeFileDeletionTo(ZoneFile* zoneFile,
                                  std::string *output, std::string linkf);

        Status DecodeSnapshotFrom(Slice *input);
        Status DecodeFileUpdateFrom(Slice *slice, bool replace = false);
        Status DecodeFileDeletionFrom(Slice *slice);

        Status RecoverFrom(ZenMetaLog *log);

        std::string ToAuxPath(std::string path)
        {
            return superblock_->GetAuxFsPath() + path;
        }

        std::string ToZenFSPath(std::string aux_path)
        {
            std::string path = aux_path;
            path.erase(0, superblock_->GetAuxFsPath().length());
            return path;
        }

        /* Must hold files_mtx_ */
        ZoneFile* GetFileNoLock(std::string fname);
        /* Must hold files_mtx_ */
        void GetZenFSChildrenNoLock(const std::string &dir,
                                    bool include_grandchildren,
                                    std::vector<std::string> *result);
        /* Must hold files_mtx_ */
        Status GetChildrenNoLock(const std::string &dir,
                                   std::vector<std::string> *result);

        /* Must hold files_mtx_ */
        Status RenameChildNoLock(std::string const &source_dir,
                                   std::string const &dest_dir,
                                   std::string const &child);

        /* Must hold files_mtx_ */
        Status RollbackAuxDirRenameNoLock(
            const std::string &source_path, const std::string &dest_path,
            const std::vector<std::string> &renamed_children);

        /* Must hold files_mtx_ */
        Status RenameAuxPathNoLock(const std::string &source_path,
                                     const std::string &dest_path);

        /* Must hold files_mtx_ */
        Status RenameFileNoLock(const std::string &f, const std::string &t);

        ZoneFile* GetFile(std::string fname);

        /* Must hold files_mtx_, On successful return,
         * caller must release files_mtx_ and call ResetUnusedIOZones() */
        Status DeleteFileNoLock(std::string fname);

        Status Repair();

        /* Must hold files_mtx_ */
        Status DeleteDirRecursiveNoLock(const std::string &d);

        /* Must hold files_mtx_ */
        Status IsDirectoryNoLock(const std::string& path,
                                    bool* is_dir) {
            if (GetFileNoLock(path) != nullptr) {
                *is_dir = false;
                return Status::OK();
            }
            return Status::NotFound("ZenFS", "Not a Directory");
        }


    protected:
        Status OpenWritableFile(const std::string &fname,
                                WritableFile **result,
                                bool reopen);

    public:

        explicit ZenFS(ZonedBlockDevice *zbd, Logger *logger);
        virtual ~ZenFS();

        Status Mount(bool readonly);
        Status MkFS(std::string aux_fs_path, uint32_t finish_threshold);
        std::map<std::string, Env::WriteLifeTimeHint> GetWriteLifeTimeHints();

        const char *Name() const
        {
            return "ZenFS - The Zoned-enabled File System";
        }

        void EncodeJson(std::ostream &json_stream);

        void ReportSuperblock(std::string *report) { superblock_->GetReport(report); }

        virtual Status
        NewSequentialFile(const std::string &fname,
                        SequentialFile **result);

        virtual Status
        NewRandomAccessFile(const std::string &fname,
                            RandomAccessFile **result);

        virtual Status
        NewWritableFile(const std::string &fname,
                        WritableFile **result);

        virtual bool
        FileExists(const std::string &fname);

        virtual Status
        GetChildren(const std::string &dir,
                    std::vector<std::string> *result);

        virtual Status
        DeleteFile(const std::string &fname);

        virtual Status
        LinkFile(const std::string &src,
                 const std::string &target);

        virtual Status
        GetFileSize(const std::string &fname, uint64_t *file_size);

        virtual Status
        RenameFile(const std::string &src,
                   const std::string &target);

        Status GetFreeSpace(const std::string & /*path*/,
                            uint64_t *diskfree)
        {
            *diskfree = zbd_->GetFreeSpace();
            return Status::OK();
        }

        Status GetFileModificationTime(const std::string &fname,
                                       uint64_t *mtime);

        // The directory structure is stored in the aux file system

        Status CreateDir(const std::string &d)
        {
            return target()->CreateDir(ToAuxPath(d));
        }

        Status CreateDirIfMissing(const std::string &d)
        {
            return target()->CreateDirIfMissing(ToAuxPath(d));
        }

        Status DeleteDir(const std::string &d)
        {
            std::vector<std::string> children;
            Status s;

            s = GetChildren(d, &children);
            if (children.size() != 0)
                return Status::IOError("Directory has children");

            return target()->DeleteDir(ToAuxPath(d));
        }

        Status DeleteDirRecursive(const std::string &d);

        Status LockFile(const std::string &f,
                          FileLock **l)
        {
            return target()->LockFile(ToAuxPath(f), l);
        }

        Status UnlockFile(FileLock *l)
        {
            return target()->UnlockFile(l);
        }

        Status GetTestDirectory(std::string *path)
        {
            *path = "rocksdbtest";
            return target()->CreateDirIfMissing(ToAuxPath(*path));
        }

        Status NewLogger(const std::string &fname, 
                         Logger **result)
        {
            return target()->NewLogger(ToAuxPath(fname), result);
        }

        void GetZenFSSnapshot(ZenFSSnapshot &snapshot,
                              const ZenFSSnapshotOptions &options);

        Status MigrateExtents(const std::vector<ZoneExtentSnapshot *> &extents);

        Status MigrateFileExtents(
            const std::string &fname,
            const std::vector<ZoneExtentSnapshot *> &migrate_exts);
    };

} // namespace leveldb
