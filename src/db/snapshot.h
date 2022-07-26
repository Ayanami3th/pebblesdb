// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SNAPSHOT_H_
#define STORAGE_LEVELDB_DB_SNAPSHOT_H_

#include "pebblesdb/db.h"
#include "db/io_zenfs.h"
#include "db/fs_zenfs.h"

namespace leveldb
{

  // Indicate what stats info we want.
  struct ZenFSSnapshotOptions
  {
    // Global zoned device stats info
    bool zbd_ = 0;
    // Per zone stats info
    bool zone_ = 0;
    // Get all file->extents & extent->file mappings
    bool zone_file_ = 0;
    bool trigger_report_ = 0;
    bool log_garbage_ = 0;
    bool as_lock_free_as_possible_ = 1;
  };

  class ZBDSnapshot
  {
  public:
    uint64_t free_space;
    uint64_t used_space;
    uint64_t reclaimable_space;

  public:
    ZBDSnapshot() = default;
    ZBDSnapshot(const ZBDSnapshot &) = default;
    ZBDSnapshot(ZonedBlockDevice &zbd)
        : free_space(zbd.GetFreeSpace()),
          used_space(zbd.GetUsedSpace()),
          reclaimable_space(zbd.GetReclaimableSpace()) {}
  };

  class ZoneSnapshot
  {
  public:
    uint64_t start;
    uint64_t wp;

    uint64_t capacity;
    uint64_t used_capacity;
    uint64_t max_capacity;

  public:
    ZoneSnapshot(const Zone &zone)
        : start(zone.start_),
          wp(zone.wp_),
          capacity(zone.capacity_),
          used_capacity(zone.used_capacity_),
          max_capacity(zone.max_capacity_) {}
  };

  class ZoneExtentSnapshot
  {
  public:
    uint64_t start;
    uint64_t length;
    uint64_t zone_start;
    std::string filename;

  public:
    ZoneExtentSnapshot(const ZoneExtent &extent, const std::string fname)
        : start(extent.start_),
          length(extent.length_),
          zone_start(extent.zone_->start_),
          filename(fname) {}
  };

  class ZoneFileSnapshot
  {
  public:
    uint64_t file_id;
    std::string filename;
    std::vector<ZoneExtentSnapshot> extents;

  public:
    ZoneFileSnapshot(ZoneFile &file)
        : file_id(file.GetID()), filename(file.GetFilename())
    {
      for (const auto *extent : file.GetExtents())
      {
        extents.emplace_back(*extent, filename);
      }
    }
  };

  class ZenFSSnapshot
  {
  public:
    ZenFSSnapshot() {}

    ZenFSSnapshot &operator=(ZenFSSnapshot &&snapshot)
    {
      zbd_ = snapshot.zbd_;
      zones_ = std::move(snapshot.zones_);
      zone_files_ = std::move(snapshot.zone_files_);
      extents_ = std::move(snapshot.extents_);
      return *this;
    }

  public:
    ZBDSnapshot zbd_;
    std::vector<ZoneSnapshot> zones_;
    std::vector<ZoneFileSnapshot> zone_files_;
    std::vector<ZoneExtentSnapshot> extents_;
  };

  class SnapshotList;

  // Snapshots are kept in a doubly-linked list in the DB.
  // Each SnapshotImpl corresponds to a particular sequence number.
  class SnapshotImpl : public Snapshot
  {
  public:
    SnapshotImpl()
        : number_(),
          prev_(NULL),
          next_(NULL),
          list_(NULL)
    {
    }
    SequenceNumber number_; // const after creation

  private:
    SnapshotImpl(const SnapshotImpl &);
    SnapshotImpl &operator=(const SnapshotImpl &);
    friend class SnapshotList;

    // SnapshotImpl is kept in a doubly-linked circular list
    SnapshotImpl *prev_;
    SnapshotImpl *next_;

    SnapshotList *list_; // just for sanity checks
  };

  class SnapshotList
  {
  public:
    SnapshotList()
        : list_()
    {
      list_.prev_ = &list_;
      list_.next_ = &list_;
    }

    bool empty() const { return list_.next_ == &list_; }
    SnapshotImpl *oldest() const
    {
      assert(!empty());
      return list_.next_;
    }
    SnapshotImpl *newest() const
    {
      assert(!empty());
      return list_.prev_;
    }

    const SnapshotImpl *New(SequenceNumber seq)
    {
      SnapshotImpl *s = new SnapshotImpl;
      s->number_ = seq;
      s->list_ = this;
      s->next_ = &list_;
      s->prev_ = list_.prev_;
      s->prev_->next_ = s;
      s->next_->prev_ = s;
      return s;
    }

    void Delete(const SnapshotImpl *s)
    {
      assert(s->list_ == this);
      s->prev_->next_ = s->next_;
      s->next_->prev_ = s->prev_;
      delete s;
    }

  private:
    // Dummy head of doubly-linked list of snapshots
    SnapshotImpl list_;
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_SNAPSHOT_H_
