// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/fs_zenfs.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sstream>
#include <fstream>
#include <utility>
#include <vector>
#include <algorithm>

#include "db/snapshot.h"
#include "util/coding.h"
#include "util/crc32c.h"

#define DEFAULT_ZENV_LOG_PATH "/tmp/"

namespace leveldb
{
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
        bool success = GenerateRfcUuid(&result);
        // if (!success) {
        //     // Fall back on our own way of generating a unique ID and adapt it to
        //     // RFC 4122 variant 1 version 4 (a random ID).
        //     // https://en.wikipedia.org/wiki/Universally_unique_identifier
        //     // We already tried GenerateRfcUuid so no need to try it again in
        //     // GenerateRawUniqueId
        //     constexpr bool exclude_port_uuid = true;
        //     uint64_t upper, lower;
        //     GenerateRawUniqueId(&upper, &lower, exclude_port_uuid);

        //     // Set 4-bit version to 4
        //     upper = (upper & (~uint64_t{0xf000})) | 0x4000;
        //     // Set unary-encoded variant to 1 (0b10)
        //     lower = (lower & (~(uint64_t{3} << 62))) | (uint64_t{2} << 62);

        //     // Use 36 character format of RFC 4122
        //     result.resize(36U);
        //     char* buf = &result[0];
        //     PutBaseChars<16>(&buf, 8, upper >> 32, /*!uppercase*/ false);
        //     *(buf++) = '-';
        //     PutBaseChars<16>(&buf, 4, upper >> 16, /*!uppercase*/ false);
        //     *(buf++) = '-';
        //     PutBaseChars<16>(&buf, 4, upper, /*!uppercase*/ false);
        //     *(buf++) = '-';
        //     PutBaseChars<16>(&buf, 4, lower >> 48, /*!uppercase*/ false);
        //     *(buf++) = '-';
        //     PutBaseChars<16>(&buf, 12, lower, /*!uppercase*/ false);
        //     assert(buf == &result[36]);

        //     // Verify variant 1 version 4
        //     assert(result[14] == '4');
        //     assert(result[19] == '8' || result[19] == '9' || result[19] == 'a' ||
        //         result[19] == 'b');
        // }
        return result;
    }

    Status Superblock::DecodeFrom(Slice *input)
    {
        if (input->size() != ENCODED_SIZE)
        {
            return Status::Corruption("ZenFS Superblock",
                                      "Error: Superblock size missmatch");
        }

        GetFixed32(input, &magic_);
        memcpy(&uuid_, input->data(), sizeof(uuid_));
        input->remove_prefix(sizeof(uuid_));
        GetFixed32(input, &sequence_);
        GetFixed32(input, &superblock_version_);
        GetFixed32(input, &flags_);
        GetFixed32(input, &block_size_);
        GetFixed32(input, &zone_size_);
        GetFixed32(input, &nr_zones_);
        GetFixed32(input, &finish_treshold_);
        memcpy(&aux_fs_path_, input->data(), sizeof(aux_fs_path_));
        input->remove_prefix(sizeof(aux_fs_path_));
        memcpy(&zenfs_version_, input->data(), sizeof(zenfs_version_));
        input->remove_prefix(sizeof(zenfs_version_));
        memcpy(&reserved_, input->data(), sizeof(reserved_));
        input->remove_prefix(sizeof(reserved_));
        assert(input->size() == 0);

        if (magic_ != MAGIC)
            return Status::Corruption("ZenFS Superblock", "Error: Magic missmatch");
        if (superblock_version_ != CURRENT_SUPERBLOCK_VERSION)
        {
            return Status::Corruption(
                "ZenFS Superblock",
                "Error: Incompatible ZenFS on-disk format version, "
                "please migrate data or switch to previously used ZenFS version. "
                "See the ZenFS README for instructions.");
        }

        return Status::OK();
    }

    void Superblock::EncodeTo(std::string *output)
    {
        sequence_++; /* Ensure that this superblock representation is unique */
        output->clear();
        PutFixed32(output, magic_);
        output->append(uuid_, sizeof(uuid_));
        PutFixed32(output, sequence_);
        PutFixed32(output, superblock_version_);
        PutFixed32(output, flags_);
        PutFixed32(output, block_size_);
        PutFixed32(output, zone_size_);
        PutFixed32(output, nr_zones_);
        PutFixed32(output, finish_treshold_);
        output->append(aux_fs_path_, sizeof(aux_fs_path_));
        output->append(zenfs_version_, sizeof(zenfs_version_));
        output->append(reserved_, sizeof(reserved_));
        assert(output->length() == ENCODED_SIZE);
    }

    void Superblock::GetReport(std::string *reportString)
    {
        reportString->append("Magic:\t\t\t\t");
        PutFixed32(reportString, magic_);
        reportString->append("\nUUID:\t\t\t\t");
        reportString->append(uuid_);
        reportString->append("\nSequence Number:\t\t");
        reportString->append(std::to_string(sequence_));
        reportString->append("\nSuperblock Version:\t\t");
        reportString->append(std::to_string(superblock_version_));
        reportString->append("\nFlags [Decimal]:\t\t");
        reportString->append(std::to_string(flags_));
        reportString->append("\nBlock Size [Bytes]:\t\t");
        reportString->append(std::to_string(block_size_));
        reportString->append("\nZone Size [Blocks]:\t\t");
        reportString->append(std::to_string(zone_size_));
        reportString->append("\nNumber of Zones:\t\t");
        reportString->append(std::to_string(nr_zones_));
        reportString->append("\nFinish Threshold [%]:\t\t");
        reportString->append(std::to_string(finish_treshold_));
        reportString->append("\nAuxiliary FS Path:\t\t");
        reportString->append(aux_fs_path_);
        reportString->append("\nZenFS Version:\t\t\t");
        std::string zenfs_version = zenfs_version_;
        if (zenfs_version.length() == 0)
        {
            zenfs_version = "Not Available";
        }
        reportString->append(zenfs_version);
    }

    Status Superblock::CompatibleWith(ZonedBlockDevice *zbd)
    {
        if (block_size_ != zbd->GetBlockSize())
            return Status::Corruption("ZenFS Superblock",
                                      "Error: block size missmatch");
        if (zone_size_ != (zbd->GetZoneSize() / block_size_))
            return Status::Corruption("ZenFS Superblock", "Error: zone size missmatch");
        if (nr_zones_ > zbd->GetNrZones())
            return Status::Corruption("ZenFS Superblock",
                                      "Error: nr of zones missmatch");

        return Status::OK();
    }

    Status ZenMetaLog::AddRecord(const Slice &slice)
    {
        uint32_t record_sz = slice.size();
        const char *data = slice.data();
        size_t phys_sz;
        uint32_t crc = 0;
        char *buffer;
        int ret;
        Status s;

        phys_sz = record_sz + zMetaHeaderSize;

        if (phys_sz % bs_)
            phys_sz += bs_ - phys_sz % bs_;

        assert(data != nullptr);
        assert((phys_sz % bs_) == 0);

        ret = posix_memalign((void **)&buffer, sysconf(_SC_PAGESIZE), phys_sz);
        if (ret)
            return Status::IOError("Failed to allocate memory");

        memset(buffer, 0, phys_sz);

        crc = crc32c::Extend(crc, (const char *)&record_sz, sizeof(uint32_t));
        crc = crc32c::Extend(crc, data, record_sz);
        crc = crc32c::Mask(crc);

        EncodeFixed32(buffer, crc);
        EncodeFixed32(buffer + sizeof(uint32_t), record_sz);
        memcpy(buffer + sizeof(uint32_t) * 2, data, record_sz);

        s = zone_->Append(buffer, phys_sz);

        free(buffer);
        return s;
    }

    Status ZenMetaLog::Read(Slice *slice)
    {
        int f = zbd_->GetReadFD();
        const char *data = slice->data();
        size_t read = 0;
        size_t to_read = slice->size();
        int ret;

        if (read_pos_ >= zone_->wp_)
        {
            // EOF
            slice->clear();
            return Status::OK();
        }

        if ((read_pos_ + to_read) > (zone_->start_ + zone_->max_capacity_))
        {
            return Status::IOError("Read across zone");
        }

        while (read < to_read)
        {
            ret = pread(f, (void *)(data + read), to_read - read, read_pos_);

            if (ret == -1 && errno == EINTR)
                continue;
            if (ret < 0)
                return Status::IOError("Read failed");

            read += ret;
            read_pos_ += ret;
        }

        return Status::OK();
    }

    Status ZenMetaLog::ReadRecord(Slice *record, std::string *scratch)
    {
        Slice header;
        uint32_t record_sz = 0;
        uint32_t record_crc = 0;
        uint32_t actual_crc;
        Status s;

        scratch->clear();
        record->clear();

        scratch->append(zMetaHeaderSize, 0);
        header = Slice(scratch->c_str(), zMetaHeaderSize);

        s = Read(&header);
        if (!s.ok())
            return s;

        // EOF?
        if (header.size() == 0)
        {
            record->clear();
            return Status::OK();
        }

        GetFixed32(&header, &record_crc);
        GetFixed32(&header, &record_sz);

        scratch->clear();
        scratch->append(record_sz, 0);

        *record = Slice(scratch->c_str(), record_sz);
        s = Read(record);
        if (!s.ok())
            return s;

        actual_crc = crc32c::Value((const char *)&record_sz, sizeof(uint32_t));
        actual_crc = crc32c::Extend(actual_crc, record->data(), record->size());

        if (actual_crc != crc32c::Unmask(record_crc))
        {
            return Status::IOError("Not a valid record");
        }

        /* Next record starts on a block boundary */
        if (read_pos_ % bs_)
            read_pos_ += bs_ - (read_pos_ % bs_);

        return Status::OK();
    }

    ZenFS::ZenFS(ZonedBlockDevice *zbd, Logger* logger)
    : zbd_(zbd), logger_(logger)
    {
        // Info(logger_, "ZenFS initializing");
        // Info(logger_, "ZenFS parameters: block device: %s, aux filesystem: %s",
        //      zbd_->GetFilename().c_str(), target()->Name());

        // Info(logger_, "ZenFS initializing");
        next_file_id_ = 1;
        metadata_writer_.zenFS = this;
    }
    
    ZenFS::~ZenFS()
    {
        Status s;
        // Info(logger_, "ZenFS shutting down");
        zbd_->LogZoneUsage();
        LogFiles();

        meta_log_.reset(nullptr);
        ClearFiles();
        delete zbd_;
    }

    Status ZenFS::Repair()
    {
        std::map<std::string, ZoneFile*>::iterator it;
        for (it = files_.begin(); it != files_.end(); it++)
        {
            ZoneFile *zFile = it->second;
            if (zFile->HasActiveExtent())
            {
                Status s = zFile->Recover();
                if (!s.ok())
                    return s;
            }
        }

        return Status::OK();
    }

    std::string ZenFS::FormatPathLexically(fs::path filepath)
    {
        fs::path ret = fs::path("/") / filepath.lexically_normal();
        return ret.string();
    }

    void ZenFS::LogFiles()
    {
        std::map<std::string, ZoneFile*>::iterator it;
        uint64_t total_size = 0;

        // Info(logger_, "  Files:\n");
        for (it = files_.begin(); it != files_.end(); it++)
        {
            ZoneFile* zFile = it->second;
            std::vector<ZoneExtent *> extents = zFile->GetExtents();

            // Info(logger_, "    %-45s sz: %lu lh: %d sparse: %u", it->first.c_str(),
            //      zFile->GetFileSize(), zFile->GetWriteLifeTimeHint(),
            //      zFile->IsSparse());
            for (unsigned int i = 0; i < extents.size(); i++)
            {
                ZoneExtent *extent = extents[i];
                // Info(logger_, "          Extent %u {start=0x%lx, zone=%u, len=%lu} ", i,
                //      extent->start_,
                //      (uint32_t)(extent->zone_->start_ / zbd_->GetZoneSize()),
                //      extent->length_);

                total_size += extent->length_;
            }
        }
        // Info(logger_, "Sum of all files: %lu MB of data \n",
        //      total_size / (1024 * 1024));
    }

    void ZenFS::ClearFiles()
    {
        std::map<std::string, ZoneFile*>::iterator it;
        std::lock_guard<std::mutex> file_lock(files_mtx_);
        // for (it = files_.begin(); it != files_.end(); it++)
        //     it->second.reset();
        files_.clear();
    }

    /* Assumes that files_mutex_ is held */
    Status ZenFS::WriteSnapshotLocked(ZenMetaLog *meta_log)
    {
        Status s;
        std::string snapshot;

        EncodeSnapshotTo(&snapshot);
        s = meta_log->AddRecord(snapshot);
        if (s.ok())
        {
            for (auto it = files_.begin(); it != files_.end(); it++)
            {
                ZoneFile* zoneFile = it->second;
                zoneFile->MetadataSynced();
            }
        }
        return s;
    }

    Status ZenFS::WriteEndRecord(ZenMetaLog *meta_log)
    {
        std::string endRecord;

        PutFixed32(&endRecord, kEndRecord);
        return meta_log->AddRecord(endRecord);
    }

    /* Assumes the files_mtx_ is held */
    Status ZenFS::RollMetaZoneLocked()
    {
        std::unique_ptr<ZenMetaLog> new_meta_log, old_meta_log;
        Zone *new_meta_zone = nullptr;
        Status s;

        ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_ROLL_LATENCY,
                                       Env::Default());
        zbd_->GetMetrics()->ReportQPS(ZENFS_ROLL_QPS, 1);

        Status status = zbd_->AllocateMetaZone(&new_meta_zone);
        if (!status.ok())
            return status;

        if (!new_meta_zone)
        {
            assert(false); // TMP
            // Error(logger_, "Out of metadata zones, we should go to read only now.");
            return Status::IOError("Out of metadata zones");
        }

        // Info(logger_, "Rolling to metazone %d\n", (int)new_meta_zone->GetZoneNr());
        new_meta_log.reset(new ZenMetaLog(zbd_, new_meta_zone));

        old_meta_log.swap(meta_log_);
        meta_log_.swap(new_meta_log);

        /* Write an end record and finish the meta data zone if there is space left */
        if (old_meta_log->GetZone()->GetCapacityLeft())
            WriteEndRecord(old_meta_log.get());
        if (old_meta_log->GetZone()->GetCapacityLeft())
            old_meta_log->GetZone()->Finish();

        std::string super_string;
        superblock_->EncodeTo(&super_string);

        s = meta_log_->AddRecord(super_string);
        if (!s.ok())
        {
            // Error(logger_,
            //       "Could not write super block when rolling to a new meta zone");
            return Status::IOError("Failed writing a new superblock");
        }

        s = WriteSnapshotLocked(meta_log_.get());

        /* We've rolled successfully, we can reset the old zone now */
        if (s.ok())
            old_meta_log->GetZone()->Reset();

        return s;
    }

    Status ZenFS::PersistSnapshot(ZenMetaLog *meta_writer)
    {
        Status s;

        std::lock_guard<std::mutex> file_lock(files_mtx_);
        std::lock_guard<std::mutex> metadata_lock(metadata_sync_mtx_);

        s = WriteSnapshotLocked(meta_writer);
        if (s == Status::IOError("No Space"))
        {
            // Info(logger_, "Current meta zone full, rolling to next meta zone");
            s = RollMetaZoneLocked();
        }

        if (!s.ok())
        {
            // Error(logger_,
            //       "Failed persisting a snapshot, we should go to read only now!");
        }

        return s;
    }

    Status ZenFS::PersistRecord(std::string record)
    {
        Status s;

        std::lock_guard<std::mutex> lock(metadata_sync_mtx_);
        s = meta_log_->AddRecord(record);
        if (s == Status::IOError("No Space"))
        {
            // Info(logger_, "Current meta zone full, rolling to next meta zone");
            s = RollMetaZoneLocked();
            /* After a successfull roll, a complete snapshot has been persisted
             * - no need to write the record update */
        }

        return s;
    }

    Status ZenFS::SyncFileExtents(ZoneFile *zoneFile,
                                    std::vector<ZoneExtent *> new_extents)
    {
        Status s;

        std::vector<ZoneExtent *> old_extents = zoneFile->GetExtents();
        zoneFile->ReplaceExtentList(new_extents);
        zoneFile->MetadataUnsynced();
        s = SyncFileMetadata(zoneFile, true);

        if (!s.ok())
        {
            return s;
        }

        // Clear changed extents' zone stats
        for (size_t i = 0; i < new_extents.size(); ++i)
        {
            ZoneExtent *old_ext = old_extents[i];
            if (old_ext->start_ != new_extents[i]->start_)
            {
                old_ext->zone_->used_capacity_ -= old_ext->length_;
            }
            delete old_ext;
        }

        return Status::OK();
    }

    /* Must hold files_mtx_ */
    Status ZenFS::SyncFileMetadataNoLock(ZoneFile *zoneFile, bool replace)
    {
        std::string fileRecord;
        std::string output;
        Status s;
        ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_META_SYNC_LATENCY,
                                       Env::Default());

        if (zoneFile->IsDeleted())
        {
            // Info(logger_, "File %s has been deleted, skip sync file metadata!",
            //      zoneFile->GetFilename().c_str());
            return Status::OK();
        }

        if (replace)
        {
            PutFixed32(&output, kFileReplace);
        }
        else
        {
            zoneFile->SetFileModificationTime(time(0));
            PutFixed32(&output, kFileUpdate);
        }
        zoneFile->EncodeUpdateTo(&fileRecord);
        PutLengthPrefixedSlice(&output, Slice(fileRecord));

        s = PersistRecord(output);
        if (s.ok())
            zoneFile->MetadataSynced();

        return s;
    }

    Status ZenFS::SyncFileMetadata(ZoneFile *zoneFile, bool replace)
    {
        std::lock_guard<std::mutex> lock(files_mtx_);
        return SyncFileMetadataNoLock(zoneFile, replace);
    }

    /* Must hold files_mtx_ */
    ZoneFile* ZenFS::GetFileNoLock(std::string fname)
    {
        ZoneFile* zoneFile(nullptr);
        fname = FormatPathLexically(fname);
        if (files_.find(fname) != files_.end())
        {
            zoneFile = files_[fname];
        }
        return zoneFile;
    }

    ZoneFile* ZenFS::GetFile(std::string fname)
    {
        ZoneFile* zoneFile(nullptr);
        std::lock_guard<std::mutex> lock(files_mtx_);
        zoneFile = GetFileNoLock(fname);
        return zoneFile;
    }

    /* Must hold files_mtx_ */
    Status ZenFS::DeleteFileNoLock(std::string fname)
    {
        ZoneFile* zoneFile(nullptr);
        Status s;

        fname = FormatPathLexically(fname);
        zoneFile = GetFileNoLock(fname);
        if (zoneFile != nullptr)
        {
            std::string record;

            files_.erase(fname);
            s = zoneFile->RemoveLinkName(fname);
            if (!s.ok())
                return s;
            EncodeFileDeletionTo(zoneFile, &record, fname);
            s = PersistRecord(record);
            if (!s.ok())
            {
                /* Failed to persist the delete, return to a consistent state */
                files_.insert(std::make_pair(fname.c_str(), zoneFile));
                zoneFile->AddLinkName(fname);
            }
            else
            {
                if (zoneFile->GetNrLinks() > 0)
                    return s;
                /* Mark up the file as deleted so it won't be migrated by GC */
                zoneFile->SetDeleted();
                // zoneFile.reset();
            }
        }
        else
        {
            s = target()->DeleteFile(ToAuxPath(fname));
        }

        return s;
    }

    Status ZenFS::NewSequentialFile(const std::string &filename,
                                    SequentialFile **result)
    {
        std::string fname = FormatPathLexically(filename);
        ZoneFile* zoneFile = GetFile(fname);

        // Debug(logger_, "New sequential file: %s direct: %d\n", fname.c_str(),
        //       file_opts.use_direct_reads);

        if (zoneFile == nullptr)
        {
            return target()->NewSequentialFile(ToAuxPath(fname), result);
        }

        *result = new ZonedSequentialFile(zoneFile);
        return Status::OK();
    }

    Status ZenFS::NewRandomAccessFile(const std::string &filename,
                                      RandomAccessFile **result)
    {
        std::string fname = FormatPathLexically(filename);
        ZoneFile* zoneFile = GetFile(fname);

        // Debug(logger_, "New random access file: %s direct: %d\n", fname.c_str(),
        //       file_opts.use_direct_reads);

        if (zoneFile == nullptr)
        {
            return target()->NewRandomAccessFile(ToAuxPath(fname), result);
        }

        *result = new ZonedRandomAccessFile(files_[fname]);
        return Status::OK();
    }

    inline bool ends_with(std::string const &value, std::string const &ending)
    {
        if (ending.size() > value.size())
            return false;
        return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
    }

    Status ZenFS::NewWritableFile(const std::string &filename,
                                  WritableFile **result)
    {
        std::string fname = FormatPathLexically(filename);
        // Debug(logger_, "New writable file: %s direct: %d\n", fname.c_str(),
        //       file_opts.use_direct_writes);

        return OpenWritableFile(fname, result, false);
    }

    bool ZenFS::FileExists(const std::string &filename)
    {
        std::string fname = FormatPathLexically(filename);
        // Debug(logger_, "FileExists: %s \n", fname.c_str());

        if (GetFile(fname) == nullptr)
        {
            return target()->FileExists(ToAuxPath(fname));
        }
        else
        {
            return true;
        }
    }


    /* Must hold files_mtx_ */
    void ZenFS::GetZenFSChildrenNoLock(const std::string &dir,
                                       bool include_grandchildren,
                                       std::vector<std::string> *result)
    {
        auto path_as_string_with_separator_at_end = [](fs::path const &path)
        {
            fs::path with_sep = path / fs::path("");
            return with_sep.lexically_normal().string();
        };

        auto string_starts_with = [](std::string const &string,
                                     std::string const &needle)
        {
            return string.rfind(needle, 0) == 0;
        };

        std::string dir_with_terminating_seperator =
            path_as_string_with_separator_at_end(fs::path(dir));

        auto relative_child_path =
            [&dir_with_terminating_seperator](std::string const &full_path)
        {
            return full_path.substr(dir_with_terminating_seperator.length());
        };

        for (auto const &it : files_)
        {
            fs::path file_path(it.first);
            assert(file_path.has_filename());

            std::string file_dir =
                path_as_string_with_separator_at_end(file_path.parent_path());

            if (string_starts_with(file_dir, dir_with_terminating_seperator))
            {
                if (include_grandchildren ||
                    file_dir.length() == dir_with_terminating_seperator.length())
                {
                    result->push_back(relative_child_path(file_path.string()));
                }
            }
        }
    }

    /* Must hold files_mtx_ */
    Status ZenFS::GetChildrenNoLock(const std::string &dir_path,
                                    std::vector<std::string> *result)
    {
        std::vector<std::string> auxfiles;
        std::string dir = FormatPathLexically(dir_path);
        Status s;

        // Debug(logger_, "GetChildrenNoLock: %s \n", dir.c_str());

        s = target()->GetChildren(ToAuxPath(dir), &auxfiles);
        if (!s.ok())
        {
            /* On ZenFS empty directories cannot be created, therefore we cannot
               distinguish between "Directory not found" and "Directory is empty"
               and always return empty lists with OK status in both cases. */
            if (s.IsNotFound())
            {
                return Status::OK();
            }
            return s;
        }

        for (const auto &f : auxfiles)
        {
            if (f != "." && f != "..")
                result->push_back(f);
        }

        GetZenFSChildrenNoLock(dir, false, result);

        return s;
    }

    Status ZenFS::GetChildren(const std::string &dir,
                                std::vector<std::string> *result)
    {
        std::lock_guard<std::mutex> lock(files_mtx_);
        return GetChildrenNoLock(dir, result);
    }

    /* Must hold files_mtx_ */
    Status ZenFS::DeleteDirRecursiveNoLock(const std::string &dir)
    {
        std::vector<std::string> children;
        std::string d = FormatPathLexically(dir);
        Status s;

        // Debug(logger_, "DeleteDirRecursiveNoLock: %s aux: %s\n", d.c_str(),
        //       ToAuxPath(d).c_str());

        s = GetChildrenNoLock(d, &children);
        if (!s.ok())
        {
            return s;
        }

        for (const auto &child : children)
        {
            std::string file_to_delete = (fs::path(d) / fs::path(child)).string();
            bool is_dir;

            s = IsDirectoryNoLock(file_to_delete, &is_dir);
            if (!s.ok())
            {
                return s;
            }

            if (is_dir)
            {
                s = DeleteDirRecursiveNoLock(file_to_delete);
            }
            else
            {
                s = DeleteFileNoLock(file_to_delete);
            }
            if (!s.ok())
            {
                return s;
            }
        }

        return target()->DeleteDir(ToAuxPath(d));
    }

    Status ZenFS::DeleteDirRecursive(const std::string &d)
    {
        Status s;
        {
            std::lock_guard<std::mutex> lock(files_mtx_);
            s = DeleteDirRecursiveNoLock(d);
        }
        if (s.ok())
            s = zbd_->ResetUnusedIOZones();
        return s;
    }

    Status ZenFS::OpenWritableFile(const std::string &filename,
                                   WritableFile **result,
                                   bool reopen)
    {
        Status s;
        std::string fname = FormatPathLexically(filename);
        bool resetIOZones = false;
        {
            std::lock_guard<std::mutex> file_lock(files_mtx_);
            ZoneFile* zoneFile = GetFileNoLock(fname);

            /* if reopen is true and the file exists, return it */
            if (reopen && zoneFile != nullptr)
            {
                zoneFile->AcquireWRLock();
                *result = new ZonedWritableFile(zbd_, true, zoneFile);
                return Status::OK();
            }

            if (zoneFile != nullptr)
            {
                s = DeleteFileNoLock(fname);
                if (!s.ok())
                    return s;
                resetIOZones = true;
            }

            zoneFile = new ZoneFile(zbd_, next_file_id_++, &metadata_writer_);
            zoneFile->SetFileModificationTime(time(0));
            zoneFile->AddLinkName(fname);

            /* RocksDB does not set the right io type(!)*/
            if (ends_with(fname, ".log"))
            {
                // todo
                zoneFile->SetIOType(IOType::kWAL);
                zoneFile->SetSparse(true);
            }
            else
            {
                // todo
                zoneFile->SetIOType(IOType::kUnknown);
            }

            /* Persist the creation of the file */
            s = SyncFileMetadataNoLock(zoneFile);
            if (!s.ok())
            {
                // zoneFile.reset();
                return s;
            }

            zoneFile->AcquireWRLock();
            files_.insert(std::make_pair(fname.c_str(), zoneFile));
            *result = new ZonedWritableFile(zbd_, true, zoneFile);
        }

        if (resetIOZones)
            s = zbd_->ResetUnusedIOZones();

        return s;
    }

    Status ZenFS::DeleteFile(const std::string &fname)
    {
        Status s;

        // Debug(logger_, "DeleteFile: %s \n", fname.c_str());

        files_mtx_.lock();
        s = DeleteFileNoLock(fname);
        files_mtx_.unlock();
        if (s.ok())
            s = zbd_->ResetUnusedIOZones();
        zbd_->LogZoneStats();

        return s;
    }

    Status ZenFS::GetFileModificationTime(const std::string &filename,
                                            uint64_t *mtime)
    {
        ZoneFile* zoneFile(nullptr);
        std::string f = FormatPathLexically(filename);
        Status s;

        // Debug(logger_, "GetFileModificationTime: %s \n", f.c_str());
        std::lock_guard<std::mutex> lock(files_mtx_);
        if (files_.find(f) != files_.end())
        {
            zoneFile = files_[f];
            *mtime = (uint64_t)zoneFile->GetFileModificationTime();
        }
        else
        {
            s = target()->GetFileModificationTime(ToAuxPath(f), mtime);
        }
        return s;
    }

    Status ZenFS::GetFileSize(const std::string &filename,
                              uint64_t *size)
    {
        ZoneFile* zoneFile(nullptr);
        std::string f = FormatPathLexically(filename);
        Status s;

        // Debug(logger_, "GetFileSize: %s \n", f.c_str());

        std::lock_guard<std::mutex> lock(files_mtx_);
        if (files_.find(f) != files_.end())
        {
            zoneFile = files_[f];
            *size = zoneFile->GetFileSize();
        }
        else
        {
            s = target()->GetFileSize(ToAuxPath(f), size);
        }

        return s;
    }

    /* Must hold files_mtx_ */
    Status ZenFS::RenameChildNoLock(std::string const &source_dir,
                                      std::string const &dest_dir,
                                      std::string const &child)
    {
        std::string source_child = (fs::path(source_dir) / fs::path(child)).string();
        std::string dest_child = (fs::path(dest_dir) / fs::path(child)).string();
        return RenameFileNoLock(source_child, dest_child);
    }

    /* Must hold files_mtx_ */
    Status ZenFS::RollbackAuxDirRenameNoLock(
        const std::string &source_path, const std::string &dest_path,
        const std::vector<std::string> &renamed_children)
    {
        Status s;

        for (const auto &rollback_child : renamed_children)
        {
            s = RenameChildNoLock(dest_path, source_path, rollback_child);
            if (!s.ok())
            {
                return Status::Corruption(
                    "RollbackAuxDirRenameNoLock: Failed to roll back directory rename");
            }
        }

        s = target()->RenameFile(ToAuxPath(dest_path), ToAuxPath(source_path));
        if (!s.ok())
        {
            return Status::Corruption(
                "RollbackAuxDirRenameNoLock: Failed to roll back auxiliary path "
                "renaming");
        }

        return s;
    }

    /* Must hold files_mtx_ */
    Status ZenFS::RenameAuxPathNoLock(const std::string &source_path,
                                        const std::string &dest_path)
    {
        Status s;
        std::vector<std::string> children;
        std::vector<std::string> renamed_children;

        s = target()->RenameFile(ToAuxPath(source_path), ToAuxPath(dest_path));
        if (!s.ok())
        {
            return s;
        }

        GetZenFSChildrenNoLock(source_path, true, &children);

        for (const auto &child : children)
        {
            s = RenameChildNoLock(source_path, dest_path, child);
            if (!s.ok())
            {
                Status failed_rename = s;
                s = RollbackAuxDirRenameNoLock(source_path, dest_path, renamed_children);
                if (!s.ok())
                {
                    return s;
                }
                return failed_rename;
            }
            renamed_children.push_back(child);
        }

        return s;
    }

    /* Must hold files_mtx_ */
    Status ZenFS::RenameFileNoLock(const std::string &src_path,
                                     const std::string &dst_path)
    {
        ZoneFile* source_file(nullptr);
        ZoneFile* existing_dest_file(nullptr);
        std::string source_path = FormatPathLexically(src_path);
        std::string dest_path = FormatPathLexically(dst_path);
        Status s;

        // Debug(logger_, "Rename file: %s to : %s\n", source_path.c_str(),
        //       dest_path.c_str());

        source_file = GetFileNoLock(source_path);
        if (source_file != nullptr)
        {
            existing_dest_file = GetFileNoLock(dest_path);
            if (existing_dest_file != nullptr)
            {
                s = DeleteFileNoLock(dest_path);
                if (!s.ok())
                {
                    return s;
                }
            }

            s = source_file->RenameLink(source_path, dest_path);
            if (!s.ok())
                return s;
            files_.erase(source_path);

            files_.insert(std::make_pair(dest_path, source_file));

            s = SyncFileMetadataNoLock(source_file);
            if (!s.ok())
            {
                /* Failed to persist the rename, roll back */
                files_.erase(dest_path);
                s = source_file->RenameLink(dest_path, source_path);
                if (!s.ok())
                    return s;
                files_.insert(std::make_pair(source_path, source_file));
            }
        }
        else
        {
            s = RenameAuxPathNoLock(source_path, dest_path);
        }

        return s;
    }

    Status ZenFS::RenameFile(const std::string &source_path,
                               const std::string &dest_path)
    {
        Status s;
        {
            std::lock_guard<std::mutex> lock(files_mtx_);
            s = RenameFileNoLock(source_path, dest_path);
        }
        if (s.ok())
            s = zbd_->ResetUnusedIOZones();
        return s;
    }

    Status ZenFS::LinkFile(const std::string &file, const std::string &link)
    {
        ZoneFile* src_file(nullptr);
        std::string fname = FormatPathLexically(file);
        std::string lname = FormatPathLexically(link);
        Status s;

        // Debug(logger_, "LinkFile: %s to %s\n", fname.c_str(), lname.c_str());
        {
            std::lock_guard<std::mutex> lock(files_mtx_);

            if (GetFileNoLock(lname) != nullptr)
                return Status::InvalidArgument("Failed to create link, target exists");

            src_file = GetFileNoLock(fname);
            if (src_file != nullptr)
            {
                src_file->AddLinkName(lname);
                files_.insert(std::make_pair(lname, src_file));
                s = SyncFileMetadataNoLock(src_file);
                if (!s.ok())
                {
                    s = src_file->RemoveLinkName(lname);
                    if (!s.ok())
                        return s;
                    files_.erase(lname);
                }
                return s;
            }
        }
        s = target()->LinkFile(ToAuxPath(fname), ToAuxPath(lname));
        return s;
    }

    void ZenFS::EncodeSnapshotTo(std::string *output)
    {
        std::map<std::string, ZoneFile*>::iterator it;
        std::string files_string;
        PutFixed32(output, kCompleteFilesSnapshot);
        for (it = files_.begin(); it != files_.end(); it++)
        {
            std::string file_string;
            ZoneFile* zFile = it->second;

            zFile->EncodeSnapshotTo(&file_string);
            PutLengthPrefixedSlice(&files_string, Slice(file_string));
        }
        PutLengthPrefixedSlice(output, Slice(files_string));
    }

    void ZenFS::EncodeJson(std::ostream &json_stream)
    {
        bool first_element = true;
        json_stream << "[";
        for (const auto &file : files_)
        {
            if (first_element)
            {
                first_element = false;
            }
            else
            {
                json_stream << ",";
            }
            file.second->EncodeJson(json_stream);
        }
        json_stream << "]";
    }

    Status ZenFS::DecodeFileUpdateFrom(Slice *slice, bool replace)
    {
        ZoneFile* update(new ZoneFile(zbd_, 0, &metadata_writer_));
        uint64_t id;
        Status s;

        s = update->DecodeFrom(slice);
        if (!s.ok())
            return s;

        id = update->GetID();
        if (id >= next_file_id_)
            next_file_id_ = id + 1;

        /* Check if this is an update or an replace to an existing file */
        for (auto it = files_.begin(); it != files_.end(); it++)
        {
            ZoneFile* zFile = it->second;
            if (id == zFile->GetID())
            {
                for (const auto &name : zFile->GetLinkFiles())
                {
                    if (files_.find(name) != files_.end())
                        files_.erase(name);
                    else
                        return Status::Corruption("DecodeFileUpdateFrom: missing link file");
                }

                s = zFile->MergeUpdate(update, replace);
                // update.reset();

                if (!s.ok())
                    return s;

                for (const auto &name : zFile->GetLinkFiles())
                    files_.insert(std::make_pair(name, zFile));

                return Status::OK();
            }
        }

        /* The update is a new file */
        assert(GetFile(update->GetFilename()) == nullptr);
        files_.insert(std::make_pair(update->GetFilename(), update));

        return Status::OK();
    }

    Status ZenFS::DecodeSnapshotFrom(Slice *input)
    {
        Slice slice;

        assert(files_.size() == 0);

        while (GetLengthPrefixedSlice(input, &slice))
        {
            ZoneFile* zoneFile(
                new ZoneFile(zbd_, 0, &metadata_writer_));
            Status s = zoneFile->DecodeFrom(&slice);
            if (!s.ok())
                return s;

            if (zoneFile->GetID() >= next_file_id_)
                next_file_id_ = zoneFile->GetID() + 1;

            for (const auto &name : zoneFile->GetLinkFiles())
                files_.insert(std::make_pair(name, zoneFile));
        }

        return Status::OK();
    }

    void ZenFS::EncodeFileDeletionTo(ZoneFile* zoneFile,
                                     std::string *output, std::string linkf)
    {
        std::string file_string;

        PutFixed64(&file_string, zoneFile->GetID());
        PutLengthPrefixedSlice(&file_string, Slice(linkf));

        PutFixed32(output, kFileDeletion);
        PutLengthPrefixedSlice(output, Slice(file_string));
    }

    Status ZenFS::DecodeFileDeletionFrom(Slice *input)
    {
        uint64_t fileID;
        std::string fileName;
        Slice slice;
        Status s;

        if (!GetFixed64(input, &fileID))
            return Status::Corruption("Zone file deletion: file id missing");

        if (!GetLengthPrefixedSlice(input, &slice))
            return Status::Corruption("Zone file deletion: file name missing");

        fileName = slice.ToString();
        if (files_.find(fileName) == files_.end())
            return Status::Corruption("Zone file deletion: no such file");

        ZoneFile* zoneFile = files_[fileName];
        if (zoneFile->GetID() != fileID)
            return Status::Corruption("Zone file deletion: file ID missmatch");

        files_.erase(fileName);
        s = zoneFile->RemoveLinkName(fileName);
        if (!s.ok())
            return Status::Corruption("Zone file deletion: file links missmatch");

        return Status::OK();
    }

    Status ZenFS::RecoverFrom(ZenMetaLog *log)
    {
        bool at_least_one_snapshot = false;
        std::string scratch;
        uint32_t tag = 0;
        Slice record;
        Slice data;
        Status s;
        bool done = false;

        while (!done)
        {
            Status rs = log->ReadRecord(&record, &scratch);
            if (!rs.ok())
            {
                // Error(logger_, "Read recovery record failed with error: %s",
                //       rs.ToString().c_str());
                return Status::Corruption("ZenFS", "Metadata corruption");
            }

            if (!GetFixed32(&record, &tag))
                break;

            if (tag == kEndRecord)
                break;

            if (!GetLengthPrefixedSlice(&record, &data))
            {
                return Status::Corruption("ZenFS", "No recovery record data");
            }

            switch (tag)
            {
            case kCompleteFilesSnapshot:
                ClearFiles();
                s = DecodeSnapshotFrom(&data);
                if (!s.ok())
                {
                    // Warn(logger_, "Could not decode complete snapshot: %s",
                    //      s.ToString().c_str());
                    return s;
                }
                at_least_one_snapshot = true;
                break;

            case kFileUpdate:
                s = DecodeFileUpdateFrom(&data);
                if (!s.ok())
                {
                    // Warn(logger_, "Could not decode file snapshot: %s",
                    //      s.ToString().c_str());
                    return s;
                }
                break;

            case kFileReplace:
                s = DecodeFileUpdateFrom(&data, true);
                if (!s.ok())
                {
                    // Warn(logger_, "Could not decode file snapshot: %s",
                    //      s.ToString().c_str());
                    return s;
                }
                break;

            case kFileDeletion:
                s = DecodeFileDeletionFrom(&data);
                if (!s.ok())
                {
                    // Warn(logger_, "Could not decode file deletion: %s",
                    //      s.ToString().c_str());
                    return s;
                }
                break;

            default:
                // Warn(logger_, "Unexpected metadata record tag: %u", tag);
                return Status::Corruption("ZenFS", "Unexpected tag");
            }
        }

        if (at_least_one_snapshot)
            return Status::OK();
        else
            return Status::NotFound("ZenFS", "No snapshot found");
    }

    /* Mount the filesystem by recovering form the latest valid metadata zone */
    Status ZenFS::Mount(bool readonly)
    {
        std::vector<Zone *> metazones = zbd_->GetMetaZones();
        std::vector<std::unique_ptr<Superblock>> valid_superblocks;
        std::vector<std::unique_ptr<ZenMetaLog>> valid_logs;
        std::vector<Zone *> valid_zones;
        std::vector<std::pair<uint32_t, uint32_t>> seq_map;

        Status s;

        /* We need a minimum of two non-offline meta data zones */
        if (metazones.size() < 2)
        {
            // Error(logger_,
            //       "Need at least two non-offline meta zones to open for write");
            return Status::NotSupported("Not supported 1111111");
        }

        /* Find all valid superblocks */
        for (const auto z : metazones)
        {
            std::unique_ptr<ZenMetaLog> log;
            std::string scratch;
            Slice super_record;

            if (!z->Acquire())
            {
                assert(false);
                return Status::IOError("Could not aquire busy flag of zone" +
                                       std::to_string(z->GetZoneNr()));
            }

            // log takes the ownership of z's busy flag.
            log.reset(new ZenMetaLog(zbd_, z));

            if (!log->ReadRecord(&super_record, &scratch).ok())
                continue;

            if (super_record.size() == 0)
                continue;

            std::unique_ptr<Superblock> super_block;

            super_block.reset(new Superblock());
            s = super_block->DecodeFrom(&super_record);
            if (s.ok())
                s = super_block->CompatibleWith(zbd_);
            if (!s.ok())
                return s;

            // Info(logger_, "Found OK superblock in zone %lu seq: %u\n", z->GetZoneNr(),
            //      super_block->GetSeq());

            seq_map.push_back(std::make_pair(super_block->GetSeq(), seq_map.size()));
            valid_superblocks.push_back(std::move(super_block));
            valid_logs.push_back(std::move(log));
            valid_zones.push_back(z);
        }

        if (!seq_map.size())
            return Status::NotFound("No valid superblock found");

        /* Sort superblocks by descending sequence number */
        std::sort(seq_map.begin(), seq_map.end(),
                  std::greater<std::pair<uint32_t, uint32_t>>());

        bool recovery_ok = false;
        unsigned int r = 0;

        /* Recover from the zone with the highest superblock sequence number.
           If that fails go to the previous as we might have crashed when rolling
           metadata zone.
        */
        for (const auto &sm : seq_map)
        {
            uint32_t i = sm.second;
            std::string scratch;
            std::unique_ptr<ZenMetaLog> log = std::move(valid_logs[i]);

            s = RecoverFrom(log.get());
            if (!s.ok())
            {
                if (s.IsNotFound())
                {
                    // Warn(logger_,
                    //      "Did not find a valid snapshot, trying next meta zone. Error: %s",
                    //      s.ToString().c_str());
                    continue;
                }

                // Error(logger_, "Metadata corruption. Error: %s", s.ToString().c_str());
                return s;
            }

            r = i;
            recovery_ok = true;
            meta_log_ = std::move(log);
            break;
        }

        if (!recovery_ok)
        {
            return Status::IOError("Failed to mount filesystem");
        }

        // Info(logger_, "Recovered from zone: %d", (int)valid_zones[r]->GetZoneNr());
        superblock_ = std::move(valid_superblocks[r]);
        zbd_->SetFinishTreshold(superblock_->GetFinishTreshold());

        s = target()->CreateDirIfMissing(superblock_->GetAuxFsPath());
        if (!s.ok())
        {
            // Error(logger_, "Failed to create aux filesystem directory.");
            return s;
        }

        /* Free up old metadata zones, to get ready to roll */
        for (const auto &sm : seq_map)
        {
            uint32_t i = sm.second;
            /* Don't reset the current metadata zone */
            if (i != r)
            {
                /* Metadata zones are not marked as having valid data, so they can be
                 * reset */
                valid_logs[i].reset();
            }
        }

        s = Repair();
        if (!s.ok())
            return s;

        if (readonly)
        {
            // Info(logger_, "Mounting READ ONLY");
        }
        else
        {
            std::lock_guard<std::mutex> lock(files_mtx_);
            s = RollMetaZoneLocked();
            if (!s.ok())
            {
                // Error(logger_, "Failed to roll metadata zone.");
                return s;
            }
        }

        // Info(logger_, "Superblock sequence %d", (int)superblock_->GetSeq());
        // Info(logger_, "Finish threshold %u", superblock_->GetFinishTreshold());
        // Info(logger_, "Filesystem mount OK");

        if (!readonly)
        {
            // Info(logger_, "Resetting unused IO Zones..");
            Status status = zbd_->ResetUnusedIOZones();
            if (!status.ok())
                return status;
            // Info(logger_, "  Done");
        }

        LogFiles();

        return Status::OK();
    }

    Status ZenFS::MkFS(std::string aux_fs_p, uint32_t finish_threshold)
    {
        std::vector<Zone *> metazones = zbd_->GetMetaZones();
        std::unique_ptr<ZenMetaLog> log;
        Zone *meta_zone = nullptr;
        std::string aux_fs_path = FormatPathLexically(aux_fs_p);
        Status s;

        if (aux_fs_path.length() > 255)
        {
            return Status::InvalidArgument(
                "Aux filesystem path must be less than 256 bytes\n");
        }

        ClearFiles();
        Status status = zbd_->ResetUnusedIOZones();
        if (!status.ok())
            return status;

        for (const auto mz : metazones)
        {
            if (!mz->Acquire())
            {
                assert(false);
                return Status::IOError("Could not aquire busy flag of zone " +
                                       std::to_string(mz->GetZoneNr()));
            }

            if (mz->Reset().ok())
            {
                if (!meta_zone)
                    meta_zone = mz;
            }
            else
            {
                // Warn(logger_, "Failed to reset meta zone\n");
            }

            if (meta_zone != mz)
            {
                // for meta_zone == mz the ownership of mz's busy flag is passed to log.
                if (!mz->Release())
                {
                    assert(false);
                    return Status::IOError("Could not unset busy flag of zone " +
                                           std::to_string(mz->GetZoneNr()));
                }
            }
        }

        if (!meta_zone)
        {
            return Status::IOError("Not available meta zones\n");
        }

        log.reset(new ZenMetaLog(zbd_, meta_zone));

        Superblock super(zbd_, aux_fs_path, finish_threshold);
        std::string super_string;
        super.EncodeTo(&super_string);

        s = log->AddRecord(super_string);
        if (!s.ok())
            return std::move(s);

        /* Write an empty snapshot to make the metadata zone valid */
        s = PersistSnapshot(log.get());
        if (!s.ok())
        {
            // Error(logger_, "Failed to persist snapshot: %s", s.ToString().c_str());
            return Status::IOError("Failed persist snapshot");
        }

        // Info(logger_, "Empty filesystem created");
        return Status::OK();
    }

    std::map<std::string, Env::WriteLifeTimeHint> ZenFS::GetWriteLifeTimeHints()
    {
        std::map<std::string, Env::WriteLifeTimeHint> hint_map;

        for (auto it = files_.begin(); it != files_.end(); it++)
        {
            ZoneFile* zoneFile = it->second;
            std::string filename = it->first;
            hint_map.insert(std::make_pair(filename, zoneFile->GetWriteLifeTimeHint()));
        }

        return hint_map;
    }

#if !defined(NDEBUG) || defined(WITH_TERARKDB)
    static std::string GetLogFilename(std::string bdev)
    {
        std::ostringstream ss;
        time_t t = time(0);
        struct tm *log_start = std::localtime(&t);
        char buf[40];

        std::strftime(buf, sizeof(buf), "%Y-%m-%d_%H:%M:%S.log", log_start);
        ss << DEFAULT_ZENV_LOG_PATH << std::string("zenfs_") << bdev << "_" << buf;

        return ss.str();
    }
#endif

    Status NewZenFS(const std::string &bdevname,
                    ZenFSMetrics* metrics)
    {
        Logger *logger;
        Status s;

        // TerarkDB needs to log important information in production while ZenFS
        // doesn't (currently).
        //
        // TODO(guokuankuan@bytedance.com) We need to figure out how to reuse
        // RocksDB's logger in the future.
#if !defined(NDEBUG) || defined(WITH_TERARKDB)
        s = Env::Default()->NewLogger(GetLogFilename(bdevname), &logger);
        if (!s.ok())
        {
            fprintf(stderr, "ZenFS: Could not create logger");
        }
        else
        {
            // logger->SetInfoLogLevel(DEBUG_LEVEL);
// #ifdef WITH_TERARKDB
//             logger->SetInfoLogLevel(INFO_LEVEL);
// #endif
        }
#endif

        ZonedBlockDevice *zbd = new ZonedBlockDevice(bdevname, logger, metrics);
        Status zbd_status = zbd->Open(false, true);
        if (!zbd_status.ok())
        {
            // Error(logger, "mkfs: Failed to open zoned block device: %s",
            //       zbd_status.ToString().c_str());
            return Status::IOError(zbd_status.ToString());
        }

        ZenFS *zenFS = new ZenFS(zbd, logger);
        s = zenFS->Mount(false);
        if (!s.ok())
        {
            delete zenFS;
            return s;
        }

        // *fs = zenFS;
        return Status::OK();
    }

    Status ListZenFileSystems(std::map<std::string, std::string> &out_list)
    {
        std::map<std::string, std::string> zenFileSystems;

        auto closedirDeleter = [](DIR *d)
        {
            if (d != nullptr)
                closedir(d);
        };
        std::unique_ptr<DIR, decltype(closedirDeleter)> dir{
            opendir("/sys/class/block"), std::move(closedirDeleter)};
        struct dirent *entry;

        while (NULL != (entry = readdir(dir.get())))
        {
            if (entry->d_type == DT_LNK)
            {
                std::string zbdName = std::string(entry->d_name);
                std::unique_ptr<ZonedBlockDevice> zbd{
                    new ZonedBlockDevice(zbdName, nullptr)};
                Status zbd_status = zbd->Open(true, false);

                if (zbd_status.ok())
                {
                    std::vector<Zone *> metazones = zbd->GetMetaZones();
                    std::string scratch;
                    Slice super_record;
                    Status s;

                    for (const auto z : metazones)
                    {
                        Superblock super_block;
                        std::unique_ptr<ZenMetaLog> log;
                        if (!z->Acquire())
                        {
                            return Status::IOError("Could not aquire busy flag of zone" +
                                                   std::to_string(z->GetZoneNr()));
                        }
                        log.reset(new ZenMetaLog(zbd.get(), z));

                        if (!log->ReadRecord(&super_record, &scratch).ok())
                            continue;
                        s = super_block.DecodeFrom(&super_record);
                        if (s.ok())
                        {
                            /* Map the uuid to the device-mapped (i.g dm-linear) block device to
                               avoid trying to mount the whole block device in case of a split
                               device */
                            if (zenFileSystems.find(super_block.GetUUID()) !=
                                    zenFileSystems.end() &&
                                zenFileSystems[super_block.GetUUID()].rfind("dm-", 0) == 0)
                            {
                                break;
                            }
                            zenFileSystems[super_block.GetUUID()] = zbdName;
                            break;
                        }
                    }
                    continue;
                }
            }
        }

        out_list = std::move(zenFileSystems);
        return Status::OK();
    }

    void ZenFS::GetZenFSSnapshot(ZenFSSnapshot &snapshot,
                                 const ZenFSSnapshotOptions &options)
    {
        if (options.zbd_)
        {
            snapshot.zbd_ = ZBDSnapshot(*zbd_);
        }
        if (options.zone_)
        {
            zbd_->GetZoneSnapshot(snapshot.zones_);
        }
        if (options.zone_file_)
        {
            std::lock_guard<std::mutex> file_lock(files_mtx_);
            for (const auto &file_it : files_)
            {
                ZoneFile &file = *(file_it.second);

                /* Skip files open for writing, as extents are being updated */
                if (!file.TryAcquireWRLock())
                    continue;

                // file -> extents mapping
                snapshot.zone_files_.emplace_back(file);
                // extent -> file mapping
                for (auto *ext : file.GetExtents())
                {
                    snapshot.extents_.emplace_back(*ext, file.GetFilename());
                }

                file.ReleaseWRLock();
            }
        }

        if (options.trigger_report_)
        {
            zbd_->GetMetrics()->ReportSnapshot(snapshot);
        }

        if (options.log_garbage_)
        {
            zbd_->LogGarbageInfo();
        }
    }

    Status ZenFS::MigrateExtents(
        const std::vector<ZoneExtentSnapshot *> &extents)
    {
        Status s;
        // Group extents by their filename
        std::map<std::string, std::vector<ZoneExtentSnapshot *>> file_extents;
        for (auto *ext : extents)
        {
            std::string fname = ext->filename;
            // We only migrate SST file extents
            if (ends_with(fname, ".sst"))
            {
                file_extents[fname].emplace_back(ext);
            }
        }

        for (const auto &it : file_extents)
        {
            s = MigrateFileExtents(it.first, it.second);
            if (!s.ok())
                break;
            s = zbd_->ResetUnusedIOZones();
            if (!s.ok())
                break;
        }
        return s;
    }

    Status ZenFS::MigrateFileExtents(
        const std::string &fname,
        const std::vector<ZoneExtentSnapshot *> &migrate_exts)
    {
        Status s = Status::OK();
        // Info(logger_, "MigrateFileExtents, fname: %s, extent count: %lu",
        //      fname.data(), migrate_exts.size());

        // The file may be deleted by other threads, better double check.
        auto zfile = GetFile(fname);
        if (zfile == nullptr)
        {
            return Status::OK();
        }

        // Don't migrate open for write files and prevent write reopens while we
        // migrate
        if (!zfile->TryAcquireWRLock())
        {
            return Status::OK();
        }

        std::vector<ZoneExtent *> new_extent_list;
        std::vector<ZoneExtent *> extents = zfile->GetExtents();
        for (const auto *ext : extents)
        {
            new_extent_list.push_back(
                new ZoneExtent(ext->start_, ext->length_, ext->zone_));
        }

        // Modify the new extent list
        for (ZoneExtent *ext : new_extent_list)
        {
            // Check if current extent need to be migrated
            auto it = std::find_if(migrate_exts.begin(), migrate_exts.end(),
                                   [&](const ZoneExtentSnapshot *ext_snapshot)
                                   {
                                       return ext_snapshot->start == ext->start_ &&
                                              ext_snapshot->length == ext->length_;
                                   });

            if (it == migrate_exts.end())
            {
                // Info(logger_, "Migrate extent not found, ext_start: %lu", ext->start_);
                continue;
            }

            Zone *target_zone = nullptr;

            // Allocate a new migration zone.
            s = zbd_->TakeMigrateZone(&target_zone, zfile->GetWriteLifeTimeHint(),
                                      ext->length_);
            if (!s.ok())
            {
                continue;
            }

            if (target_zone == nullptr)
            {
                zbd_->ReleaseMigrateZone(target_zone);
                // Info(logger_, "Migrate Zone Acquire Failed, Ignore Task.");
                continue;
            }

            uint64_t target_start = target_zone->wp_;
            if (zfile->IsSparse())
            {
                // For buffered write, ZenFS use inlined metadata for extents and each
                // extent has a SPARSE_HEADER_SIZE.
                target_start = target_zone->wp_ + ZoneFile::SPARSE_HEADER_SIZE;
                zfile->MigrateData(ext->start_ - ZoneFile::SPARSE_HEADER_SIZE,
                                   ext->length_ + ZoneFile::SPARSE_HEADER_SIZE,
                                   target_zone);
                zbd_->AddGCBytesWritten(ext->length_ + ZoneFile::SPARSE_HEADER_SIZE);
            }
            else
            {
                zfile->MigrateData(ext->start_, ext->length_, target_zone);
                zbd_->AddGCBytesWritten(ext->length_);
            }

            // If the file doesn't exist, skip
            if (GetFileNoLock(fname) == nullptr)
            {
                // Info(logger_, "Migrate file not exist anymore.");
                zbd_->ReleaseMigrateZone(target_zone);
                break;
            }

            ext->start_ = target_start;
            ext->zone_ = target_zone;
            ext->zone_->used_capacity_ += ext->length_;

            zbd_->ReleaseMigrateZone(target_zone);
        }

        SyncFileExtents(zfile, new_extent_list);
        zfile->ReleaseWRLock();

        // Info(logger_, "MigrateFileExtents Finished, fname: %s, extent count: %lu",
        //      fname.data(), migrate_exts.size());
        return Status::OK();
    }

} // namespace leveldb
