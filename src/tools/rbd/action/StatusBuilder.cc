// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/types.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/program_options.hpp>
#include "global/global_context.h"
#include "include/encoding.h"
#include "librbd/Types.h"
#include "cls/rbd/cls_rbd.h"
#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "include/rbd/librbdx.hpp"

namespace rbd {
namespace action {
namespace status_builder {

enum {
  v1 = 1,       // ignore du
  v2,           // include image head du
  v3,           // include snapshot du
  v4,           // include snapshot dirty
};

namespace at = argument_types;
namespace po = boost::program_options;

#define RBD_SNAP_KEY_PREFIX             "snapshot_"
#define RBD_DIR_ID_KEY_PREFIX           "id_"
#define RBD_DIR_NAME_KEY_PREFIX         "name_"

#define TRASH_IMAGE_KEY_PREFIX          "id_"

#define STATUS_VERSION_KEY              "zversion"
#define STATUS_IMAGE_KEY_PREFIX         "zimage_"
#define STATUS_SNAPSHOT_KEY_PREFIX      "zsnapshot_"

const int RBD_MAX_KEYS_READ = 64;

std::string id_obj_name(const std::string &name)
{
  return RBD_ID_PREFIX + name;
}

std::string header_name(const std::string &image_id)
{
  return RBD_HEADER_PREFIX + image_id;
}

std::string snap_key_for_id(uint64_t snap_id)
{
  ostringstream oss;
  oss << RBD_SNAP_KEY_PREFIX
      << std::setw(16) << std::setfill('0') << std::hex << snap_id;
  return oss.str();
}

uint64_t snap_id_from_key(const std::string &key)
{
  std::istringstream iss(key);
  uint64_t id;
  iss.ignore(strlen(RBD_SNAP_KEY_PREFIX)) >> std::hex >> id;
  return id;
}

std::string dir_key_for_id(const string &id)
{
  return RBD_DIR_ID_KEY_PREFIX + id;
}

std::string dir_id_from_key(const string &key)
{
  return key.substr(strlen(RBD_DIR_ID_KEY_PREFIX));
}

std::string dir_key_for_name(const string &name)
{
  return RBD_DIR_NAME_KEY_PREFIX + name;
}

std::string dir_name_from_key(const string &key)
{
  return key.substr(strlen(RBD_DIR_NAME_KEY_PREFIX));
}

std::string trash_key_for_id(const std::string &id) {
  return TRASH_IMAGE_KEY_PREFIX + id;
}

std::string trash_id_from_key(const std::string &key) {
  return key.substr(strlen(TRASH_IMAGE_KEY_PREFIX));
}

std::string status_key_for_image(const std::string &id)
{
  return STATUS_IMAGE_KEY_PREFIX + id;
}

std::string status_image_from_key(const string &key)
{
  return key.substr(strlen(STATUS_IMAGE_KEY_PREFIX));
}

std::string status_key_for_snapshot(uint64_t id)
{
  ostringstream oss;
  oss << STATUS_SNAPSHOT_KEY_PREFIX
      << std::setw(16) << std::setfill('0') << std::hex << id;
  return oss.str();
}

uint64_t status_snapshot_from_key(const std::string &key)
{
  std::istringstream iss(key);
  uint64_t id;
  iss.ignore(strlen(STATUS_SNAPSHOT_KEY_PREFIX)) >> std::hex >> id;
  return id;
}

void get_check_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
  options->add_options()
      ("v1", po::bool_switch(), "check status records, du ignored");
  options->add_options()
      ("v2", po::bool_switch(), "check status records, image head du included");
  options->add_options()
      ("v3", po::bool_switch(), "check status records, snapshot du included");
  options->add_options()
      ("v4", po::bool_switch(), "check status records, snapshot dirty included");
  options->add_options()
      ("rebuild", po::bool_switch(), "rebuild status record if inconsistency exists");
  options->add_options()
      ("purge", po::bool_switch(), "purge status records");
}

template<typename T>
int read_key(librados::IoCtx &ioctx, const std::string &oid,
    const std::string &key, T *out) {
  std::set<std::string> keys;
  std::map<std::string, bufferlist> vals;

  keys.insert(key);
  int r = ioctx.omap_get_vals_by_keys(oid, keys, &vals);
  if (r < 0) {
    return r;
  }
  try {
    auto it = vals.begin();
    if (it == vals.end()) {
      return -ENOENT;
    }
    bufferlist::iterator bl_it = it->second.begin();
    ::decode(*out, bl_it);
  } catch (const buffer::error &err) {
    return -EIO;
  }
  return 0;
}

int remove_key(librados::IoCtx &ioctx, const std::string &oid,
    const std::string &key) {
  std::set<std::string> keys;

  keys.insert(key);
  int r = ioctx.omap_rm_keys(oid, keys);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  return 0;
}

template<typename T>
int read(librados::IoCtx &ioctx, const std::string &oid, T *out) {
  uint64_t size;
  int r = ioctx.stat2(oid, &size, nullptr);
  if (r < 0) {
    std::cerr << __func__ << ": stat2: "
        << oid << " failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  if (size == 0) {
    return -ENOENT;
  }

  bufferlist bl;
  r = ioctx.read(oid, bl, size, 0);
  if (r < 0) {
    return r;
  }
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*out, iter);
  } catch (const buffer::error &err) {
    return -EIO;
  }
  return 0;
}

int check_image_existent(librados::IoCtx &ioctx, std::string id) {
  // check if image exists in RBD_DIRECTORY
  std::string dir_key = dir_key_for_id(id);
  std::string name;
  int r = read_key(ioctx, RBD_DIRECTORY, dir_key, &name);
  if (r < 0 && r != -ENOENT) {
    std::cerr << __func__ << ": read_key: " << RBD_DIRECTORY << "/" << dir_key
        << " failed: " << cpp_strerror(r) << std::endl;
    return r;
  }

  // check if image exists in RBD_TRASH
  std::string trash_key = trash_key_for_id(id);
  cls::rbd::TrashImageSpec trash;
  int r2 = read_key(ioctx, RBD_TRASH, trash_key, &trash);
  if (r2 < 0 && r2 != -ENOENT) {
    std::cerr << __func__ << ": read_key: " << RBD_TRASH << "/" << trash_key
        << " failed: " << cpp_strerror(r2) << std::endl;
    return r2;
  }

  if (r == 0 && r2 == 0) {
    std::cerr << __func__ << ": image: " << id << ", corrupted, "
        "exists both in RBD_DIRECTORY and RBD_TRASH" << std::endl;
    return -ESTALE;
  }

  if (r == -ENOENT && r2 == -ENOENT) {
    return -ENOENT;
  }
  return 0;
}

int check_snapshot_existent(librados::IoCtx &ioctx, std::string image_id,
    uint64_t snapshot_id) {
  int r = check_image_existent(ioctx, image_id);
  if (r < 0) {
    if (r != -ENOENT) {
      std::cerr << __func__ << ": check_image_existent: "
          << image_id << " failed: "
          << cpp_strerror(r) << std::endl;
    }
    return r;
  }

  std::string oid = header_name(image_id);
  int max_read = RBD_MAX_KEYS_READ;
  string last_read = RBD_SNAP_KEY_PREFIX;
  bool more;

  do {
    std::map<std::string, bufferlist> vals;
    r = ioctx.omap_get_vals2(oid, last_read,
        RBD_SNAP_KEY_PREFIX, max_read, &vals, &more);
    if (r < 0) {
      std::cerr << __func__ << ": omap_get_vals2: "
          << RBD_SNAP_KEY_PREFIX << " failed: "
          << cpp_strerror(r) << std::endl;
      return r;
    }

    for (auto &it : vals) {
      uint64_t snap_id = snap_id_from_key(it.first);
      if (snapshot_id == snap_id) {
        return 0;
      }
    }
    if (!vals.empty()) {
      last_read = vals.rbegin()->first;
    }
  } while (more);

  return -ENOENT;
}

/*
 * list children of given parent spec
 */
int build_children(librados::IoCtx &ioctx, const librbd::ParentSpec &spec,
    std::set<cls::rbd::StatusCloneId> *children) {
  // search all pools for children depending on this snapshot
  librados::Rados rados(ioctx);
  std::list<std::pair<int64_t, std::string> > pools;
  int r = rados.pool_list2(pools);
  if (r < 0) {
    std::cerr << __func__ << ": error listing pools: "
        << cpp_strerror(r) << std::endl;
    return r;
  }

  for (auto &it : pools) {
    int64_t base_tier;
    r = rados.pool_get_base_tier(it.first, &base_tier);
    if (r == -ENOENT) {
      continue;
    }

    if (r < 0) {
      std::cerr << __func__ << ": error retrieving base tier for pool: "
          << it.second << ": " << cpp_strerror(r) << std::endl;
      return r;
    }

    if (it.first != base_tier) {
      // pool is a cache; skip it
      continue;
    }

    librados::IoCtx child_ioctx;
    r = rados.ioctx_create2(it.first, child_ioctx);
    if (r == -ENOENT) {
      continue;
    }

    if (r < 0) {
      std::cerr << __func__ << ": error accessing child image pool: "
          << it.second << ": " << cpp_strerror(r) << std::endl;
      return r;
    }

    std::set<std::string> image_ids;
    r = librbd::cls_client::get_children(&child_ioctx, RBD_CHILDREN,
        spec, image_ids);
    if (r < 0 && r != -ENOENT) {
      std::cerr << __func__ << ": error reading list of children from pool: "
          << it.second << ": " << cpp_strerror(r) << std::endl;
      return r;
    }

    for (auto &c : image_ids) {
      children->insert(cls::rbd::StatusCloneId(it.first, c));
    }
  }
  return 0;
}



/*
 * build status record of a single image
 */
int build_status_image(librados::IoCtx &ioctx, const std::string &id,
    int checkv,
    cls::rbd::StatusImage *image,
    std::map<uint64_t, cls::rbd::StatusSnapshot> *snapshots) {
  auto xrbd = librbdx::xRBD();

  if (checkv == v1) {
    librbdx::image_info_t info;
    int r = xrbd.get_info(ioctx, id, &info);
    if (r < 0) {
      return r;
    }

    image->state = cls::rbd::STATUS_IMAGE_STATE_IDLE;
    if (info.watchers.size()) {
      image->state = cls::rbd::STATUS_IMAGE_STATE_MAPPED;
    }
    image->create_timestamp = info.timestamp;
    image->parent.pool_id = info.parent.spec.pool_id;
    image->parent.image_id = info.parent.spec.image_id;
    image->parent.snapshot_id = info.parent.spec.snap_id;
    image->data_pool_id = info.data_pool_id;
    image->name = info.name;
    image->id = info.id;
    image->order = info.order;
    image->stripe_unit = info.stripe_unit;
    image->stripe_count = info.stripe_count;
    image->size = info.size;
    image->used = 0;
    image->qos_iops = info.qos.iops;
    image->qos_bps = info.qos.bps;
    // reservation and weight are ignored
    image->qos_reservation = -1;
    image->qos_weight = -1;
    image->snapshot_ids.insert(info.snapc.snaps.begin(), info.snapc.snaps.end());

    auto pool_id = ioctx.get_id();

    for (auto& it: info.snaps) {
      auto& snap_info = it.second;

      std::set<cls::rbd::StatusCloneId> children;
      librbd::ParentSpec parent_spec(pool_id, image->id, snap_info.id);
      r = build_children(ioctx, parent_spec, &children);
      if (r < 0) {
        std::cerr << __func__ << ": build_children: "
            << pool_id << "/" << id << "@" << snap_info.id << " failed: "
            << cpp_strerror(r) << std::endl;
        return r;
      }

      cls::rbd::StatusSnapshot snap;
      snap.create_timestamp = snap_info.timestamp;

      cls::rbd::SnapshotNamespace sn;
      switch (cls::rbd::SnapshotNamespaceType(snap_info.snap_ns_type)) {
        case cls::rbd::SNAPSHOT_NAMESPACE_TYPE_USER:
          sn = cls::rbd::UserSnapshotNamespace();
          break;
        default:
          sn = cls::rbd::UnknownSnapshotNamespace();
          break;
      }
      snap.snapshot_namespace = cls::rbd::SnapshotNamespaceOnDisk(sn);
      snap.name = snap_info.name;
      snap.image_id = image->id;
      snap.id = snap_info.id;
      snap.size = snap_info.size;
      snap.used = 0;
      snap.dirty = 0;
      snap.clone_ids = std::move(children);

      snapshots->insert(std::make_pair(snap.id, snap));
    }
  } else if (checkv == v2) {
    librbdx::image_info_v2_t info;
    int r = xrbd.get_info_v2(ioctx, id, &info);
    if (r < 0) {
      return r;
    }

    image->state = cls::rbd::STATUS_IMAGE_STATE_IDLE;
    if (info.watchers.size()) {
      image->state = cls::rbd::STATUS_IMAGE_STATE_MAPPED;
    }
    image->create_timestamp = info.timestamp;
    image->parent.pool_id = info.parent.spec.pool_id;
    image->parent.image_id = info.parent.spec.image_id;
    image->parent.snapshot_id = info.parent.spec.snap_id;
    image->data_pool_id = info.data_pool_id;
    image->name = info.name;
    image->id = info.id;
    image->order = info.order;
    image->stripe_unit = info.stripe_unit;
    image->stripe_count = info.stripe_count;
    image->size = info.size;
    image->used = info.du;
    image->qos_iops = info.qos.iops;
    image->qos_bps = info.qos.bps;
    // reservation and weight are ignored
    image->qos_reservation = -1;
    image->qos_weight = -1;
    image->snapshot_ids.insert(info.snapc.snaps.begin(), info.snapc.snaps.end());

    auto pool_id = ioctx.get_id();

    for (auto& it: info.snaps) {
      auto& snap_info = it.second;

      std::set<cls::rbd::StatusCloneId> children;
      librbd::ParentSpec parent_spec(pool_id, image->id, snap_info.id);
      r = build_children(ioctx, parent_spec, &children);
      if (r < 0) {
        std::cerr << __func__ << ": build_children: "
            << pool_id << "/" << id << "@" << snap_info.id << " failed: "
            << cpp_strerror(r) << std::endl;
        return r;
      }

      cls::rbd::StatusSnapshot snap;
      snap.create_timestamp = snap_info.timestamp;

      cls::rbd::SnapshotNamespace sn;
      switch (cls::rbd::SnapshotNamespaceType(snap_info.snap_ns_type)) {
        case cls::rbd::SNAPSHOT_NAMESPACE_TYPE_USER:
          sn = cls::rbd::UserSnapshotNamespace();
          break;
        default:
          sn = cls::rbd::UnknownSnapshotNamespace();
          break;
      }
      snap.snapshot_namespace = cls::rbd::SnapshotNamespaceOnDisk(sn);
      snap.name = snap_info.name;
      snap.image_id = image->id;
      snap.id = snap_info.id;
      snap.size = snap_info.size;
      snap.used = 0;
      snap.dirty = 0;
      snap.clone_ids = std::move(children);

      snapshots->insert(std::make_pair(snap.id, snap));
    }
  } else if (checkv == v3 || checkv == v4) {
    librbdx::image_info_v3_t info;
    int r = xrbd.get_info_v3(ioctx, id, &info);
    if (r < 0) {
      return r;
    }

    image->state = cls::rbd::STATUS_IMAGE_STATE_IDLE;
    if (info.watchers.size()) {
      image->state = cls::rbd::STATUS_IMAGE_STATE_MAPPED;
    }
    image->create_timestamp = info.timestamp;
    image->parent.pool_id = info.parent.spec.pool_id;
    image->parent.image_id = info.parent.spec.image_id;
    image->parent.snapshot_id = info.parent.spec.snap_id;
    image->data_pool_id = info.data_pool_id;
    image->name = info.name;
    image->id = info.id;
    image->order = info.order;
    image->stripe_unit = info.stripe_unit;
    image->stripe_count = info.stripe_count;
    image->size = info.size;
    image->used = info.du;
    image->qos_iops = info.qos.iops;
    image->qos_bps = info.qos.bps;
    // reservation and weight are ignored
    image->qos_reservation = -1;
    image->qos_weight = -1;
    image->snapshot_ids.insert(info.snapc.snaps.begin(), info.snapc.snaps.end());

    auto pool_id = ioctx.get_id();

    for (auto& it: info.snaps) {
      auto& snap_info = it.second;

      std::set<cls::rbd::StatusCloneId> children;
      librbd::ParentSpec parent_spec(pool_id, image->id, snap_info.id);
      r = build_children(ioctx, parent_spec, &children);
      if (r < 0) {
        std::cerr << __func__ << ": build_children: "
            << pool_id << "/" << id << "@" << snap_info.id << " failed: "
            << cpp_strerror(r) << std::endl;
        return r;
      }

      cls::rbd::StatusSnapshot snap;
      snap.create_timestamp = snap_info.timestamp;

      cls::rbd::SnapshotNamespace sn;
      switch (cls::rbd::SnapshotNamespaceType(snap_info.snap_ns_type)) {
        case cls::rbd::SNAPSHOT_NAMESPACE_TYPE_USER:
          sn = cls::rbd::UserSnapshotNamespace();
          break;
        default:
          sn = cls::rbd::UnknownSnapshotNamespace();
          break;
      }
      snap.snapshot_namespace = cls::rbd::SnapshotNamespaceOnDisk(sn);
      snap.name = snap_info.name;
      snap.image_id = image->id;
      snap.id = snap_info.id;
      snap.size = snap_info.size;
      snap.used = snap_info.du;
      snap.dirty = snap_info.dirty;     // only checks in v4 and above
      snap.clone_ids = std::move(children);

      snapshots->insert(std::make_pair(snap.id, snap));
    }
  } else {
    return -EINVAL;
  }
  return 0;
}

/*
 * read image status record
 */
int read_status_image(librados::IoCtx &ioctx, const std::string &id,
    cls::rbd::StatusImage *image,
    std::map<uint64_t, cls::rbd::StatusSnapshot> *snapshots) {
  std::string image_key = status_key_for_image(id);
  int r = read_key(ioctx, RBD_STATUS, image_key, image);
  if (r < 0 && r != -ENOENT) {
    std::cerr << __func__ << ": read_key: " << RBD_STATUS << "/" << image_key
        << " failed: " << cpp_strerror(r) << std::endl;
    return r;
  }

  if (r == -ENOENT) {
    return r;
  }

  for (auto &it : image->snapshot_ids) {
    std::string snapshot_key = status_key_for_snapshot(it);
    cls::rbd::StatusSnapshot snapshot;
    r = read_key(ioctx, RBD_STATUS, snapshot_key, &snapshot);
    if (r < 0 && r != -ENOENT) {
      std::cerr << __func__ << ": read_key: " << RBD_STATUS << "/" << snapshot_key
          << " failed: " << cpp_strerror(r) << std::endl;
      return r;
    }

    if (r == -ENOENT) {
      continue;
    }

    snapshots->insert(std::make_pair(it, snapshot));
  }

  return 0;
}

/*
 * iterate RBD_STATUS to check each status record
 *
 * this function only checks if the image/snapshot the status record
 * points to does exist, the consistency is checked by
 * check_directory and check_trash
 */
int check_status(librados::IoCtx &ioctx, int checkv, bool rebuild) {
  // check image records
  {
    std::string last_read = STATUS_IMAGE_KEY_PREFIX;
    int max_read = RBD_MAX_KEYS_READ;
    bool more = true;

    while (more) {
      std::map<std::string, bufferlist> vals;
      int r = ioctx.omap_get_vals2(RBD_STATUS, last_read,
          STATUS_IMAGE_KEY_PREFIX, max_read,
          &vals, &more);
      if (r < 0 && r != -ENOENT) {
        std::cerr << __func__ << ": omap_get_vals2: "
            << STATUS_IMAGE_KEY_PREFIX << " failed: "
            << cpp_strerror(r) << std::endl;
        return r;
      }

      if (r == -ENOENT) { // RBD_STATUS does not exist
        return 0;
      }

      // iterate image records
      for (auto &it : vals) {
        cls::rbd::StatusImage image;
        bufferlist::iterator iter = it.second.begin();
        try {
          ::decode(image, iter);
        } catch (const buffer::error &err) {
          return -EIO;
        }

        std::string id = status_image_from_key(it.first);
        r = check_image_existent(ioctx, id);
        if (r < 0 && r != -ENOENT) {
          std::cerr << __func__ << ": check image existent: " << id << " failed: "
              << cpp_strerror(r) << std::endl;
          return r;
        }

        if (r == -ENOENT) {
          if (rebuild) { // image does not exist, remove it from RBD_STATUS
            r = remove_key(ioctx, RBD_STATUS, it.first);
            if (r < 0) {
              std::cerr << __func__ << ": remove_key: " << RBD_STATUS << "/"
                  << it.first << " failed: "
                  << cpp_strerror(r) << std::endl;
              return r;
            }
          } else {
            std::cout << "status image: " << id << " is dangling" << std::endl;
          }

          continue;
        }
      }

      if (!vals.empty()) {
        last_read = vals.rbegin()->first;
      }
    }
  }

  // check snapshot records
  {
    std::string last_read = STATUS_SNAPSHOT_KEY_PREFIX;
    int max_read = RBD_MAX_KEYS_READ;
    bool more = true;

    while (more) {
      std::map<std::string, bufferlist> vals;
      int r = ioctx.omap_get_vals2(RBD_STATUS, last_read,
          STATUS_SNAPSHOT_KEY_PREFIX, max_read,
          &vals, &more);
      if (r < 0 && r != -ENOENT) {
        std::cerr << __func__ << ": omap_get_vals2: "
            << STATUS_SNAPSHOT_KEY_PREFIX << " failed: "
            << cpp_strerror(r) << std::endl;
        return r;
      }

      if (r == -ENOENT) { // RBD_STATUS does not exist
        return 0;
      }

      // iterate snapshot records
      for (auto &it : vals) {
        cls::rbd::StatusSnapshot snapshot;
        bufferlist::iterator iter = it.second.begin();
        try {
          ::decode(snapshot, iter);
        } catch (const buffer::error &err) {
          return -EIO;
        }

        uint64_t snapshot_id = status_snapshot_from_key(it.first);
        std::string image_id = snapshot.image_id;
        r = check_snapshot_existent(ioctx, image_id, snapshot_id);
        if (r < 0 && r != -ENOENT) {
          std::cerr << __func__ << ": check snapshot existent: "
              << image_id << "@" << snapshot_id << " failed: "
              << cpp_strerror(r) << std::endl;
          return r;
        }

        if (r == -ENOENT) {
          if (rebuild) { // snapshot does not exist, remove it from RBD_STATUS
            r = remove_key(ioctx, RBD_STATUS, it.first);
            if (r < 0) {
              std::cerr << __func__ << ": remove_key: " << RBD_STATUS << "/"
                  << it.first << " failed: "
                  << cpp_strerror(r) << std::endl;
              return r;
            }
          } else {
            std::cout << "status snapshot: " << image_id << "@" << snapshot_id
                << " is dangling" << std::endl;
          }

          continue;
        }
      }

      if (!vals.empty()) {
        last_read = vals.rbegin()->first;
      }
    }
  }
  return 0;
}

/*
 * compare two image records to check if there is any differences
 */
int compare_status_image(cls::rbd::StatusImage &image_new,
    std::map<uint64_t, cls::rbd::StatusSnapshot> &snapshots_new,
    cls::rbd::StatusImage &image_old,
    std::map<uint64_t, cls::rbd::StatusSnapshot> &snapshots_old,
    int checkv,
    Formatter *f) {
  std::string id = image_new.id;
  bool inconsistent = false;

  if (image_new.state != image_old.state) {
    inconsistent = true;
  } else

  // ignore create_timestamp, since the timestamps recorded in rbd_header
  // and RBD_STATUS are different
//  if (image_new.create_timestamp != image_old.create_timestamp) {
//    inconsistent = true;
//  } else

  if (image_new.parent.pool_id != image_old.parent.pool_id
      || image_new.parent.image_id != image_old.parent.image_id
      || image_new.parent.snapshot_id != image_old.parent.snapshot_id) {
    inconsistent = true;
  } else

  if (image_new.data_pool_id != image_old.data_pool_id) {
    inconsistent = true;
  } else

  if (image_new.name != image_old.name) {
    inconsistent = true;
  } else

  if (image_new.id != image_old.id) { // should never happen
    inconsistent = true;
  } else

  if (image_new.order != image_old.order) {
    inconsistent = true;
  } else

  if (image_new.stripe_unit != image_old.stripe_unit) {
    inconsistent = true;
  } else

  if (image_new.stripe_count != image_old.stripe_count) {
    inconsistent = true;
  } else

  if (image_new.size != image_old.size) {
    inconsistent = true;
  } else

  if (image_new.used != image_old.used) {
    if (checkv >= v2) { // check image head du
      inconsistent = true;
    }
  } else

  if (image_new.qos_iops != image_old.qos_iops) {
    inconsistent = true;
  } else

  if (image_new.qos_bps != image_old.qos_bps) {
    inconsistent = true;
  } else

  // ignore reservation and weight
//  if (image_new.qos_reservation != image_old.qos_reservation) {
//    inconsistent = true;
//  } else
//
//  if (image_new.qos_weight != image_old.qos_weight) {
//    inconsistent = true;
//  } else

  if (image_new.snapshot_ids != image_old.snapshot_ids) {
    inconsistent = true;
  }

  for (auto it = snapshots_new.begin(); it != snapshots_new.end();) {
    uint64_t snapshot_id = it->first;
    std::string snapshot_key = status_key_for_snapshot(snapshot_id);

    auto old_it = snapshots_old.find(snapshot_id);
    if (old_it == snapshots_old.end()) {
      it++;
      continue;
    }

    const cls::rbd::StatusSnapshot &snapshot_new = it->second;
    const cls::rbd::StatusSnapshot &snapshot_old = snapshots_old[snapshot_id];

    // ignore create_timestamp, since the timestamps recorded in rbd_header
    // and RBD_STATUS are different
//    if (snapshot_new.create_timestamp != snapshot_old.create_timestamp) {
//      it++;
//      continue;
//    }

    if (!(snapshot_new.snapshot_namespace == snapshot_old.snapshot_namespace)) {
      it++;
      continue;
    }

    if (snapshot_new.name != snapshot_old.name) {
      it++;
      continue;
    }

    if (snapshot_new.image_id != snapshot_old.image_id) {
      it++;
      continue;
    }

    if (snapshot_new.id != snapshot_old.id) { // should never happen
      it++;
      continue;
    }

    if (snapshot_new.size != snapshot_old.size) {
      it++;
      continue;
    }

    if (snapshot_new.used != snapshot_old.used) {
      if (checkv >= v3) { // check snapshot du
        it++;
        continue;
      }
    }

    if (snapshot_new.dirty != snapshot_old.dirty) {
      if (checkv >= v4) { // check snapshot dirty
        it++;
        continue;
      }
    }

    if (snapshot_new.clone_ids != snapshot_old.clone_ids) {
      it++;
      continue;
    }

    it = snapshots_new.erase(it);
  }

  // dump

  if (inconsistent) {
    f->open_object_section(id.c_str());

    f->open_object_section("image_new");
    image_new.dump2(f);
    f->close_section();

    f->open_object_section("image_old");
    image_old.dump2(f);
    f->close_section();
  }

  bool snapshots_inconsistent = false;
  if (!snapshots_new.empty()) {
    snapshots_inconsistent = true;

    if (!inconsistent) {
      f->open_object_section(id.c_str());
    }

    f->open_array_section("snapshots");

    for (auto &it : snapshots_new) {
      auto snapshot_id = it.first;
      ostringstream oss;
      oss << snapshot_id;
      std::string snapshot_str = oss.str();

      f->open_object_section("snapshot");

      auto old_it = snapshots_old.find(snapshot_id);
      if (old_it == snapshots_old.end()) {
        f->dump_string(snapshot_str.c_str(), "snapshot status record does not exist");
      } else {
        f->open_object_section(snapshot_str.c_str());

        f->open_object_section("new");
        it.second.dump2(f);
        f->close_section();

        f->open_object_section("old");
        old_it->second.dump2(f);
        f->close_section();

        f->close_section();
      }

      f->close_section(); // snapshot
    }

    f->close_section(); // snapshots
  }

  if (inconsistent || snapshots_inconsistent) {
    f->close_section();
    return 1;
  }
  return 0;
}

int write_status_image(librados::IoCtx &ioctx, const std::string &oid,
    cls::rbd::StatusImage &image,
    std::map<uint64_t, cls::rbd::StatusSnapshot> &snapshots) {
  std::map<std::string, bufferlist> vals;

  std::string image_key = status_key_for_image(image.id);
  ::encode(image, vals[image_key]);

  for (auto &it : snapshots) {
    std::string snapshot_key = status_key_for_snapshot(it.first);
    ::encode(it.second, vals[snapshot_key]);
  }

  int r = ioctx.create(oid, false);
  if (r < 0) {
    std::cerr << __func__ << ": create: "
        << oid << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }

  r = ioctx.omap_set(oid, vals);
  if (r < 0) {
    std::cerr << __func__ << ": omap_set: "
        << oid << "/" << image.id << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

/*
 * check status record of a single image
 */
int check_image(librados::IoCtx &ioctx, std::string id, int checkv, bool rebuild) {
  std::string dir_key = dir_key_for_id(id);
  std::string name;
  int r = read_key(ioctx, RBD_DIRECTORY, dir_key, &name);
  if (r < 0 && r != -ENOENT) {
    std::cerr << __func__ << ": read_key: " << RBD_DIRECTORY << "/" << dir_key
        << " failed: " << cpp_strerror(r) << std::endl;
    return r;
  }

  std::string trash_key = trash_key_for_id(id);
  cls::rbd::TrashImageSpec trash_spec;
  int r2 = read_key(ioctx, RBD_TRASH, trash_key, &trash_spec);
  if (r2 < 0 && r2 != -ENOENT) {
    std::cerr << __func__ << ": read_key: " << RBD_TRASH << "/" << trash_key
        << " failed: " << cpp_strerror(r2) << std::endl;
    return r2;
  }

  if (r == 0 && r2 == 0) {
    std::cerr << __func__ << ": image: " << id << ", corrupted, "
        "exists both in RBD_DIRECTORY and RBD_TRASH" << std::endl;
    return -ESTALE;
  }

  // image to check does not exist
  if (r == -ENOENT && r2 == -ENOENT) {
    return -ENOENT;
  }

  at::Format format("json");
  auto formatter = format.create_formatter(true);
  formatter->open_object_section("image");
  bool inconsistent = false;

  std::string image_name = name;
  uint64_t state = 0;
  if (r == -ENOENT) { // image in trash
    image_name = trash_spec.name;
    state = cls::rbd::STATUS_IMAGE_STATE_TRASH;
  }

  cls::rbd::StatusImage image_new;
  std::map<uint64_t, cls::rbd::StatusSnapshot> snapshots_new;
  r = build_status_image(ioctx, id, checkv, &image_new, &snapshots_new);
  if (r < 0) {
    std::cerr << __func__ << ": build_status_image: " << id << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }

  image_new.state &= ~cls::rbd::STATUS_IMAGE_STATE_TRASH;
  image_new.state |= state;
  image_new.name = image_name;

  cls::rbd::StatusImage image_old;
  std::map<uint64_t, cls::rbd::StatusSnapshot> snapshots_old;
  r = read_status_image(ioctx, id, &image_old, &snapshots_old);
  if (r < 0 && r != -ENOENT) {
    std::cerr << __func__ << ": read_status_image: " << id << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }

  if (r != -ENOENT) {
    // compare
    r = compare_status_image(image_new, snapshots_new,
        image_old, snapshots_old,
        checkv,
        formatter.get());
  } else {
    formatter->dump_string(id.c_str(), "status record does not exist");
  }

  if (r != 0) {
    inconsistent = true;

    if (rebuild) {
      r = write_status_image(ioctx, RBD_STATUS, image_new, snapshots_new);
      if (r < 0) {
        std::cerr << __func__ << ": write_status_image: " << id << " failed: "
            << cpp_strerror(r) << std::endl;
        return r;
      }
    }
  }

  formatter->close_section();

  if (inconsistent && !rebuild) {
    formatter->flush(std::cout);
  }

  return 0;
}

/*
 * iterate RBD_DIRECTORY to check each image
 */
int check_directory(librados::IoCtx &ioctx, int checkv, bool rebuild) {
  int r, ret = 0;
  std::string last_read = RBD_DIR_ID_KEY_PREFIX;
  int max_read = RBD_MAX_KEYS_READ;
  bool more = true;

  at::Format format("json");
  auto formatter = format.create_formatter(true);
  formatter->open_object_section("directory");
  bool inconsistent = false;

  while (more) {
    std::map<std::string, bufferlist> vals;
    r = ioctx.omap_get_vals2(RBD_DIRECTORY, last_read,
        RBD_DIR_ID_KEY_PREFIX, max_read,
        &vals, &more);
    if (r < 0 && r != -ENOENT) {
      std::cerr << __func__ << ": omap_get_vals2: "
          << RBD_DIR_ID_KEY_PREFIX << " failed: "
          << cpp_strerror(r) << std::endl;
      return r;
    }

    if (r == -ENOENT) { // RBD_DIRECTORY does not exist
      return 0;
    }

    for (auto &it : vals) {
      std::string name;
      try {
        bufferlist::iterator bl_it = it.second.begin();
        ::decode(name, bl_it);
      } catch (const buffer::error &err) {
        return -EIO;
      }

      std::string id = dir_id_from_key(it.first);

      cls::rbd::StatusImage image_new;
      std::map<uint64_t, cls::rbd::StatusSnapshot> snapshots_new;
      r = build_status_image(ioctx, id, checkv, &image_new, &snapshots_new);
      if (r < 0) {
        std::cerr << __func__ << ": build_status_image: " << id << " failed: "
            << cpp_strerror(r) << std::endl;
        if (!ret) {
          ret = r;
          continue;
        }
      }

      image_new.state &= ~cls::rbd::STATUS_IMAGE_STATE_TRASH;
      image_new.name = name;

      cls::rbd::StatusImage image_old;
      std::map<uint64_t, cls::rbd::StatusSnapshot> snapshots_old;
      r = read_status_image(ioctx, id, &image_old, &snapshots_old);
      if (r < 0 && r != -ENOENT) {
        std::cerr << __func__ << ": read_status_image: " << id << " failed: "
            << cpp_strerror(r) << std::endl;
        if (!ret) {
          ret = r;
          continue;
        }
      }

      if (r != -ENOENT) {
        // compare
        r = compare_status_image(image_new, snapshots_new,
            image_old, snapshots_old,
            checkv,
            formatter.get());
      } else {
        formatter->dump_string(id.c_str(), "status record does not exist");
      }

      if (r != 0) {
        inconsistent = true;

        if (rebuild) {
          r = write_status_image(ioctx, RBD_STATUS, image_new, snapshots_new);
          if (r < 0) {
            std::cerr << __func__ << ": write_status_image: " << id << " failed: "
                << cpp_strerror(r) << std::endl;
            if (!ret) {
              ret = r;
              continue;
            }
          }
        }
      }
    }

    if (!vals.empty()) {
      last_read = vals.rbegin()->first;
    }
  }

  formatter->close_section();

  if (inconsistent && !rebuild) {
    formatter->flush(std::cout);
  }

  return ret;
}

/*
 * iterate RBD_TRASH to check each image
 */
int check_trash(librados::IoCtx &ioctx, int checkv, bool rebuild) {
  int r, ret = 0;
  std::string last_read = TRASH_IMAGE_KEY_PREFIX;
  int max_read = RBD_MAX_KEYS_READ;
  bool more = true;

  at::Format format("json");
  auto formatter = format.create_formatter(true);
  formatter->open_object_section("trash");
  bool inconsistent = false;

  while (more) {
    std::map<std::string, bufferlist> vals;
    r = ioctx.omap_get_vals2(RBD_TRASH, last_read,
        TRASH_IMAGE_KEY_PREFIX, max_read,
        &vals, &more);
    if (r < 0 && r != -ENOENT) {
      std::cerr << __func__ << ": omap_get_vals2: "
          << TRASH_IMAGE_KEY_PREFIX << " failed: "
          << cpp_strerror(r) << std::endl;
      return r;
    }

    if (r == -ENOENT) { // RBD_TRASH does not exist
      return 0;
    }

    for (auto &it : vals) {
      cls::rbd::TrashImageSpec trash_spec;
      try {
        bufferlist::iterator bl_it = it.second.begin();
        ::decode(trash_spec, bl_it);
      } catch (const buffer::error &err) {
        return -EIO;
      }

      std::string id = trash_id_from_key(it.first);

      cls::rbd::StatusImage image_new;
      std::map<uint64_t, cls::rbd::StatusSnapshot> snapshots_new;
      r = build_status_image(ioctx, id, checkv, &image_new, &snapshots_new);
      if (r < 0) {
        std::cerr << __func__ << ": build_status_image: " << id << " failed: "
            << cpp_strerror(r) << std::endl;
        if (!ret) {
          ret = r;
          continue;
        }
      }

      image_new.state |= cls::rbd::STATUS_IMAGE_STATE_TRASH;
      image_new.name = trash_spec.name;

      cls::rbd::StatusImage image_old;
      std::map<uint64_t, cls::rbd::StatusSnapshot> snapshots_old;
      r = read_status_image(ioctx, id, &image_old, &snapshots_old);
      if (r < 0 && r != -ENOENT) {
        std::cerr << __func__ << ": read_status_image: " << id << " failed: "
            << cpp_strerror(r) << std::endl;
        if (!ret) {
          ret = r;
          continue;
        }
      }

      if (r != -ENOENT) {
        // compare
        r = compare_status_image(image_new, snapshots_new,
            image_old, snapshots_old,
            checkv,
            formatter.get());
      } else {
        formatter->dump_string(id.c_str(), "status record does not exist");
      }

      if (r != 0) {
        inconsistent = true;

        if (rebuild) {
          r = write_status_image(ioctx, RBD_STATUS, image_new, snapshots_new);
          if (r < 0) {
            std::cerr << __func__ << ": write_status_image: " << id << " failed: "
                << cpp_strerror(r) << std::endl;
            if (!ret) {
              ret = r;
              continue;
            }
          }
        }
      }
    }

    if (!vals.empty()) {
      last_read = vals.rbegin()->first;;
    }
  }

  formatter->close_section();

  if (inconsistent && !rebuild) {
    formatter->flush(std::cout);
  }

  return ret;
}

int execute_check(const po::variables_map &vm) {
  std::string pool_name;
  std::string image_name;
  std::string image_id;
  size_t arg_index = 0;

  if (vm.count(at::IMAGE_ID)) {
    image_id = vm[at::IMAGE_ID].as<std::string>();
  }

  bool has_image_spec = utils::check_if_image_spec_present(
      vm, at::ARGUMENT_MODIFIER_NONE, arg_index);

  if (!image_id.empty() && has_image_spec) {
    std::cerr << "rbd: trying to check image status record using both name and id. "
              << std::endl;
    return -EINVAL;
  }

  int r;
  if (image_id.empty()) {
    r = utils::get_pool_image_snapshot_names(vm,
        at::ARGUMENT_MODIFIER_NONE, &arg_index,
        &pool_name, &image_name, nullptr,
        utils::SNAPSHOT_PRESENCE_NONE,
        utils::SPEC_VALIDATION_NONE, false);
  } else {
    r = utils::get_pool_snapshot_names(vm,
        at::ARGUMENT_MODIFIER_NONE, &arg_index,
        &pool_name, nullptr,
        utils::SNAPSHOT_PRESENCE_NONE,
        utils::SPEC_VALIDATION_NONE);
  }
  if (r < 0) {
    return r;
  }

  bool check_v1 = vm["v1"].as<bool>();
  bool check_v2 = vm["v2"].as<bool>();
  bool check_v3 = vm["v3"].as<bool>();
  bool check_v4 = vm["v4"].as<bool>();
  bool rebuild = vm["rebuild"].as<bool>();
  bool purge = vm["purge"].as<bool>();

  int checkv = 0;
  if (check_v1) {
    checkv = v1;
  }
  if (check_v2) {
    checkv = v2;
  }
  if (check_v3) {
    checkv = v3;
  }
  if (check_v4) {
    checkv = v4;
  }

  // no action
  if (!checkv && !purge) {
    std::cerr << "rbd: either '--v1/--v2/--v3' with optionally '--rebuild' "
                 "or '--purge' should be specified"
              << std::endl;
    return -EINVAL;
  }

  // w/o check, rebuild
  if (!checkv && rebuild) {
    std::cerr << "rbd: '--rebuild' should be used with '--v1/--v2/--v3'"
              << std::endl;
    return -EINVAL;
  }

  // check, w/o rebuild, purge
  if (checkv && !rebuild && purge) {
    std::cerr << "rbd: '--purge' should be used alone or used with '--rebuild'"
              << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx ioctx;

  if (!image_name.empty() || !image_id.empty()) {
    if (purge) {
      std::cerr << "rbd: purge status records is not allowed for a single image. "
                << std::endl;
      return -EINVAL;
    }

    r = utils::init(pool_name, &rados, &ioctx);
    if (r < 0) {
      return r;
    }

    std::string id = image_id;
    if (!image_name.empty()) {
      std::string oid = id_obj_name(image_name);
      r = read(ioctx, oid, &id);
      if (r < 0) {
        return r;
      }
    }

    r = check_image(ioctx, id, checkv, rebuild);
    if (r < 0) {
      std::cerr << __func__ << ": check_image: " << id << " failed: "
          << cpp_strerror(r) << std::endl;
      return r;
    }

    if (rebuild) { // always increase the version for convenient
      librbd::RBD rbd;

      r = rbd.status_inc_version(ioctx, 1);
      if (r < 0) {
        std::cerr << __func__ << ": status_inc_version failed: "
            << cpp_strerror(r) << std::endl;
        return r;
      }
    }

    return 0;
  }

  r = utils::init(pool_name, &rados, &ioctx);
  if (r < 0) {
    return r;
  }

  uint64_t version = 0;
  if (purge) { // purge omap entries, i.e., status records, on RBD_STATUS
    librbd::RBD rbd;
    r = rbd.status_get_version(ioctx, &version);
    if (r < 0 && r != -ENOENT) {
      std::cerr << __func__ << ": status_get_version failed: "
          << cpp_strerror(r) << std::endl;
      return r;
    }

    r = ioctx.omap_clear(RBD_STATUS);
    if (r < 0 && r != -ENOENT) {
      std::cerr << __func__ << ": omap_clear: " << RBD_STATUS << " failed: "
          << cpp_strerror(r) << std::endl;
      return r;
    }

    // purge only
    if (!checkv) {
      return 0;
    }
  }

  int ret = 0;
  r = check_status(ioctx, checkv, rebuild);
  if (r < 0) {
    std::cerr << __func__ << ": check_status failed: "
        << cpp_strerror(r) << std::endl;
    ret = r;
  }

  r = check_directory(ioctx, checkv, rebuild);
  if (r < 0) {
    std::cerr << __func__ << ": check_directory failed: "
        << cpp_strerror(r) << std::endl;
    if (!ret) {
      ret = r;
    }
  }

  r = check_trash(ioctx, checkv, rebuild);
  if (r < 0) {
    std::cerr << __func__ << ": check_trash failed: "
        << cpp_strerror(r) << std::endl;
    if (!ret) {
      ret = r;
    }
  }

  if (rebuild) { // always increase the version for convenient
    librbd::RBD rbd;

    version++;  // should be 1 or version before purge
    r = rbd.status_inc_version(ioctx, version);
    if (r < 0) {
      std::cerr << __func__ << ": status_inc_version failed: "
          << cpp_strerror(r) << std::endl;
      if (!ret) {
        ret = r;
      }
    }
  }

  return ret;
}

Shell::Action action_check(
    {"status-check"}, {}, "Check, rebuild or purge status records.", "",
    &get_check_arguments, &execute_check);

} // namespace status_builder
} // namespace action
} // namespace rbd
