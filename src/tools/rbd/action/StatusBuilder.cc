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

namespace rbd {
namespace action {
namespace status_builder {

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
      ("check-status", po::bool_switch(), "check status");
  options->add_options()
      ("check-directory", po::bool_switch(), "check directory");
  options->add_options()
      ("check-trash", po::bool_switch(), "check trash");
  options->add_options()
      ("rebuild", po::bool_switch(), "rebuild optionally");
  options->add_options()
      ("from-scratch", po::bool_switch(), "rebuild from scratch");
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

int build_status_children(librados::IoCtx &ioctx, const librbd::ParentSpec &spec,
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

int build_status_image(librados::IoCtx &ioctx, const std::string &id,
    cls::rbd::StatusImage *image,
    std::map<uint64_t, cls::rbd::StatusSnapshot> *snapshots) {
  image->id = id;

  std::string oid = header_name(id);

  std::string object_prefix;
  uint8_t order;
  int r = librbd::cls_client::get_immutable_metadata(&ioctx, oid,
      &object_prefix, &order);
  if (r < 0) {
    std::cerr << __func__ << ": get_immutable_metadata: " << oid << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }

  uint64_t size, features, incompatible_features;
  std::map<rados::cls::lock::locker_id_t, rados::cls::lock::locker_info_t> lockers;
  bool exclusive_lock;
  std::string lock_tag;
  ::SnapContext snapc;
  librbd::ParentInfo parent_info;
  r = librbd::cls_client::get_mutable_metadata(&ioctx, oid, false,
      &size, &features, &incompatible_features, &lockers, &exclusive_lock,
      &lock_tag, &snapc, &parent_info);
  if (r < 0) {
    std::cerr << __func__ << ": get_mutable_metadata: " << oid << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }

  if (!snapc.is_valid()) {
    std::cerr << __func__ << ": image: " << id << " has invalid snap context"
        << std::endl;
    return -EIO;
  }

  if (!snapc.snaps.empty()) {
    std::vector<std::string> snap_names;
    std::vector<uint64_t> snap_sizes;
    std::vector<librbd::ParentInfo> snap_parents;
    std::vector<uint8_t> snap_protection_statuses;
    std::vector<utime_t> snap_timestamps;
    std::vector<cls::rbd::SnapshotNamespace> snap_namespaces;

    r = librbd::cls_client::snapshot_list(&ioctx, oid, snapc.snaps,
        &snap_names, &snap_sizes, &snap_parents, &snap_protection_statuses);
    if (r < 0) {
      std::cerr << __func__ << ": snapshot_list: " << oid << " failed: "
          << cpp_strerror(r) << std::endl;
      return r;
    }
    r = librbd::cls_client::snapshot_timestamp_list(&ioctx, oid, snapc.snaps,
        &snap_timestamps);
    if (r < 0) {
      std::cerr << __func__ << ": snapshot_timestamp_list: " << oid << " failed: "
          << cpp_strerror(r) << std::endl;
      return r;
    }
    r = librbd::cls_client::snapshot_namespace_list(&ioctx, oid, snapc.snaps,
        &snap_namespaces);
    if (r < 0) {
      std::cerr << __func__ << ": snapshot_namespace_list: " << oid << " failed: "
          << cpp_strerror(r) << std::endl;
      return r;
    }

    image->snapshot_ids.insert(snapc.snaps.begin(), snapc.snaps.end());

    for (size_t i = 0; i < snapc.snaps.size(); ++i) {
      std::set<cls::rbd::StatusCloneId> children;
      snapid_t snapid = snapc.snaps[i];
      librbd::ParentSpec parent_spec(ioctx.get_id(), id, snapid);
      r = build_status_children(ioctx, parent_spec, &children);
      if (r < 0) {
        std::cerr << __func__ << ": build_children: "
            << ioctx.get_id() << "/" << id << "@" << snapid.val << " failed: "
            << cpp_strerror(r) << std::endl;
        return r;
      }

      cls::rbd::StatusSnapshot snapshot;
      snapshot.create_timestamp = snap_timestamps[i];
      snapshot.snapshot_namespace = snap_namespaces[i];
      snapshot.name = snap_names[i];
      snapshot.image_id = id;
      snapshot.id = snapid.val;
      snapshot.size = snap_sizes[i];
      snapshot.used = 0;
      snapshot.dirty = 0;
      snapshot.clone_ids = std::move(children);

      snapshots->insert(std::make_pair(snapshot.id, snapshot));
    }
  }

  utime_t create_timestamp;
  r = librbd::cls_client::get_create_timestamp(&ioctx, oid, &create_timestamp);
  if (r < 0) {
    std::cerr << __func__ << ": get_create_timestamp: " << oid << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }

  int64_t data_pool_id = -1;
  r = librbd::cls_client::get_data_pool(&ioctx, oid, &data_pool_id);
  if (r < 0) {
    std::cerr << __func__ << ": get_data_pool: " << oid << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }

  uint64_t stripe_unit = 0, stripe_count = 0;
  r = librbd::cls_client::get_stripe_unit_count(&ioctx, oid,
      &stripe_unit, &stripe_count);
  if (r < 0 && r != -ENOEXEC) {
    std::cerr << __func__ << ": get_stripe_unit_count: " << oid << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }

  std::string qos_iops_str, qos_bps_str, qos_reservation_str, qos_weight_str;
  r = librbd::cls_client::metadata_get(&ioctx, oid, QOS_MLMT, &qos_iops_str);
  if (r < 0 && r != -ENOENT) {
    std::cerr << __func__ << ": metadata_get: "
        << oid << "/" << QOS_MLMT << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }
  r = librbd::cls_client::metadata_get(&ioctx, oid, QOS_MBDW, &qos_bps_str);
  if (r < 0 && r != -ENOENT) {
    std::cerr << __func__ << ": metadata_get: "
        << oid << "/" << QOS_MBDW << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }
  r = librbd::cls_client::metadata_get(&ioctx, oid, QOS_MRSV, &qos_reservation_str);
  if (r < 0 && r != -ENOENT) {
    std::cerr << __func__ << ": metadata_get: "
        << oid << "/" << QOS_MRSV << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }
  r = librbd::cls_client::metadata_get(&ioctx, oid, QOS_MWGT, &qos_weight_str);
  if (r < 0 && r != -ENOENT) {
    std::cerr << __func__ << ": metadata_get: "
        << oid << "/" << QOS_MWGT << " failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }

  int64_t qos_iops = -1, qos_bps = -1, qos_reservation = -1, qos_weight = -1;
  if (!qos_iops_str.empty()) {
    qos_iops = std::stoll(qos_iops_str);
  }
  if (!qos_bps_str.empty()) {
    qos_bps = std::stoll(qos_bps_str);
  }
  if (!qos_reservation_str.empty()) {
    qos_reservation = std::stoll(qos_reservation_str);
  }
  if (!qos_weight_str.empty()) {
    qos_weight = std::stoll(qos_weight_str);
  }

  image->create_timestamp = create_timestamp;

  image->parent.pool_id = parent_info.spec.pool_id;
  image->parent.image_id = parent_info.spec.image_id;
  image->parent.snapshot_id = parent_info.spec.snap_id;

  image->size = size;
  image->order = order;
  image->data_pool_id = data_pool_id;

  if ((stripe_unit == 1ull << order) && (stripe_count == 1)) {
    stripe_unit = stripe_count = 0;
  }

  image->stripe_unit = stripe_unit;
  image->stripe_count = stripe_count;

  image->qos_iops = qos_iops;
  image->qos_bps = qos_bps;
  image->qos_reservation = qos_reservation;
  image->qos_weight = qos_weight;

  return 0;
}

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

int check_status_images(librados::IoCtx &ioctx, bool rebuild) {
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
        std::cerr << __func__ << ": check_image_existent: " << id << " failed: "
            << cpp_strerror(r) << std::endl;
        return r;
      }

      if (r == -ENOENT) {
        if (rebuild) {
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
  return 0;
}

int check_status_snapshots(librados::IoCtx &ioctx, bool rebuild) {
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
        std::cerr << __func__ << ": check_snapshot_existent: "
            << image_id << "@" << snapshot_id << " failed: "
            << cpp_strerror(r) << std::endl;
        return r;
      }

      if (r == -ENOENT) {
        if (rebuild) {
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
  return 0;
}

int check_status(librados::IoCtx &ioctx, bool rebuild) {
  // check_existing_status only checks if the image/snapshot exists, the
  // correctness is checked by check_images and check_trash

  int r = check_status_images(ioctx, rebuild);
  if (r < 0) {
    std::cerr << __func__ << ": check_status_images failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }

  r = check_status_snapshots(ioctx, rebuild);
  if (r < 0) {
    std::cerr << __func__ << ": check_status_snapshots failed: "
        << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

int compare_image(cls::rbd::StatusImage &image_new,
    std::map<uint64_t, cls::rbd::StatusSnapshot> &snapshots_new,
    cls::rbd::StatusImage &image_old,
    std::map<uint64_t, cls::rbd::StatusSnapshot> &snapshots_old,
    Formatter *f) {
  std::string id = image_new.id;
  bool inconsistent = false;

  uint64_t state_new, state_old;
  state_new = image_new.state & cls::rbd::STATUS_IMAGE_STATE_TRASH;
  state_old = image_old.state & cls::rbd::STATUS_IMAGE_STATE_TRASH;
  if (state_new != state_old) {
    inconsistent = true;
  } else

  // ignore create_timestamp

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

  // ignore used

  if (image_new.qos_iops != image_old.qos_iops) {
    inconsistent = true;
  } else

  if (image_new.qos_bps != image_old.qos_bps) {
    inconsistent = true;
  } else

  if (image_new.qos_reservation != image_old.qos_reservation) {
    inconsistent = true;
  } else

  if (image_new.qos_weight != image_old.qos_weight) {
    inconsistent = true;
  } else

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

    // ignore create_timestamp

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

    // ignore used

    // ignore dirty

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
        f->dump_string(snapshot_str.c_str(), "no status");
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

      f->close_section();
    }
    f->close_section();
  }

  if (inconsistent || snapshots_inconsistent) {
    f->close_section();
    return 1;
  }
  return 0;
}

int write_image(librados::IoCtx &ioctx, const std::string &oid,
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

int check_image(librados::IoCtx &ioctx, std::string id, bool rebuild) {
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

  if (r == -ENOENT && r2 == -ENOENT) {
    return -ENOENT;
  }

  at::Format format("json");
  auto formatter = format.create_formatter(true);
  formatter->open_object_section("image");
  bool inconsistent = false;

  std::string image_name = name;
  uint64_t state = 0;
  if (r == -ENOENT) {
    image_name = trash_spec.name;
    state = cls::rbd::STATUS_IMAGE_STATE_TRASH;
  }

  cls::rbd::StatusImage image_new;
  std::map<uint64_t, cls::rbd::StatusSnapshot> snapshots_new;
  r = build_status_image(ioctx, id, &image_new, &snapshots_new);
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
    r = compare_image(image_new, snapshots_new, image_old, snapshots_old,
        formatter.get());
  } else {
    formatter->dump_string(id.c_str(), "no status");
  }

  if (r != 0) {
    inconsistent = true;

    if (rebuild) {
      r = write_image(ioctx, RBD_STATUS, image_new, snapshots_new);
      if (r < 0) {
        std::cerr << __func__ << ": write_image: " << id << " failed: "
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

int check_directory(librados::IoCtx &ioctx, bool rebuild) {
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
      r = build_status_image(ioctx, id, &image_new, &snapshots_new);
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
        r = compare_image(image_new, snapshots_new, image_old, snapshots_old,
            formatter.get());
      } else {
        formatter->dump_string(id.c_str(), "no status");
      }

      if (r != 0) {
        inconsistent = true;

        if (rebuild) {
          r = write_image(ioctx, RBD_STATUS, image_new, snapshots_new);
          if (r < 0) {
            std::cerr << __func__ << ": write_image: " << id << " failed: "
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

int check_trash(librados::IoCtx &ioctx, bool rebuild) {
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
      r = build_status_image(ioctx, id, &image_new, &snapshots_new);
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
        r = compare_image(image_new, snapshots_new, image_old, snapshots_old,
            formatter.get());
      } else {
        formatter->dump_string(id.c_str(), "no status");
      }

      if (r != 0) {
        inconsistent = true;

        if (rebuild) {
          r = write_image(ioctx, RBD_STATUS, image_new, snapshots_new);
          if (r < 0) {
            std::cerr << __func__ << ": write_image: " << id << " failed: "
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
    std::cerr << "rbd: trying to check image status using both name and id. "
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

  bool all = false;
  bool status = vm["check-status"].as<bool>();
  bool directory = vm["check-directory"].as<bool>();
  bool trash = vm["check-trash"].as<bool>();
  bool rebuild = vm["rebuild"].as<bool>();
  bool from_scratch = vm["from-scratch"].as<bool>();

  if (!status && !directory && !trash) {
    all = true;
  }

  librados::Rados rados;
  librados::IoCtx ioctx;
  r = utils::init(pool_name, &rados, &ioctx);
  if (r < 0) {
    return r;
  }

  if (!image_name.empty() || !image_id.empty()) {
    std::string id = image_id;
    if (!image_name.empty()) {
      std::string oid = id_obj_name(image_name);
      r = read(ioctx, oid, &id);
      if (r < 0) {
        return r;
      }
    }

    r = check_image(ioctx, id, rebuild);
    if (r < 0) {
      std::cerr << __func__ << ": check_image: " << id << " failed: "
          << cpp_strerror(r) << std::endl;
    }
    return r;
  }

  uint64_t version = 0;
  if (rebuild && from_scratch) {
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
  }

  int ret = 0;
  if (all || status) {
    r = check_status(ioctx, rebuild);
    if (r < 0) {
      std::cerr << __func__ << ": check_status failed: "
          << cpp_strerror(r) << std::endl;
      ret = r;
    }
  }

  if (all || directory) {
    r = check_directory(ioctx, rebuild);
    if (r < 0) {
      std::cerr << __func__ << ": check_directory failed: "
          << cpp_strerror(r) << std::endl;
      if (!ret) {
        ret = r;
      }
    }
  }

  if (all || trash) {
    r = check_trash(ioctx, rebuild);
    if (r < 0) {
      std::cerr << __func__ << ": check_trash failed: "
          << cpp_strerror(r) << std::endl;
      if (!ret) {
        ret = r;
      }
    }
  }

  if (rebuild) { // always increase the version for convenient
    librbd::RBD rbd;

    version++;
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
    {"status-check"}, {}, "Check status and rebuild optionally.", "",
    &get_check_arguments, &execute_check);

} // namespace status_builder
} // namespace action
} // namespace rbd
