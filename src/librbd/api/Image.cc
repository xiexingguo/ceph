// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Image.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Image: " << __func__ << ": "

namespace librbd {
namespace api {

template <typename I>
int Image<I>::list_images(librados::IoCtx& io_ctx,
                          std::vector<image_spec_t> *images) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "list " << &io_ctx << dendl;

  int r;
  images->clear();

  bufferlist bl;
  r = io_ctx.read(RBD_DIRECTORY, bl, 0, 0);
  if (r == -ENOENT) {
    return 0;
  } else if (r < 0) {
    lderr(cct) << "error listing v1 images: " << cpp_strerror(r) << dendl;
    return r;
  }

  // V1 format images are in a tmap
  if (bl.length()) {
    auto p = bl.begin();
    bufferlist header;
    std::map<std::string, bufferlist> m;
    ::decode(header, p);
    ::decode(m, p);
    for (auto& it : m) {
      images->push_back({.id ="", .name = it.first});
    }
  }

  // V2 format images
  std::map<std::string, std::string> image_names_to_ids;
  r = list_images_v2(io_ctx, &image_names_to_ids);
  if (r < 0) {
    lderr(cct) << "error listing v2 images: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (const auto& img_pair : image_names_to_ids) {
    images->push_back({.id = img_pair.second,
                       .name = img_pair.first});
  }

  return 0;
}

template <typename I>
int Image<I>::list_images_v2(librados::IoCtx& io_ctx, ImageNameToIds *images) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  // new format images are accessed by class methods
  int r;
  int max_read = 1024;
  string last_read = "";
  do {
    map<string, string> images_page;
    r = cls_client::dir_list(&io_ctx, RBD_DIRECTORY, last_read, max_read,
                             &images_page);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error listing image in directory: "
                 << cpp_strerror(r) << dendl;
      return r;
    } else if (r == -ENOENT) {
      break;
    }
    for (map<string, string>::const_iterator it = images_page.begin();
         it != images_page.end(); ++it) {
      images->insert(*it);
    }
    if (!images_page.empty()) {
      last_read = images_page.rbegin()->first;
    }
    r = images_page.size();
  } while (r == max_read);

  return 0;
}

template <typename I>
int Image<I>::list_children(I *ictx, const ParentSpec &parent_spec,
                            PoolImageIds *pool_image_ids)
{
  CephContext *cct = ictx->cct;

  // no children for non-layered or old format image
  if (!ictx->test_features(RBD_FEATURE_LAYERING, ictx->snap_lock)) {
    return 0;
  }

  pool_image_ids->clear();
  // search all pools for children depending on this snapshot
  librados::Rados rados(ictx->md_ctx);
  std::list<std::pair<int64_t, std::string> > pools;
  int r = rados.pool_list2(pools);
  if (r < 0) {
    lderr(cct) << "error listing pools: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (auto it = pools.begin(); it != pools.end(); ++it) {
    int64_t base_tier;
    r = rados.pool_get_base_tier(it->first, &base_tier);
    if (r == -ENOENT) {
      ldout(cct, 1) << "pool " << it->second << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      lderr(cct) << "error retrieving base tier for pool " << it->second
                 << dendl;
      return r;
    }
    if (it->first != base_tier) {
      // pool is a cache; skip it
      continue;
    }

    IoCtx ioctx;
    r = rados.ioctx_create2(it->first, ioctx);
    if (r == -ENOENT) {
      ldout(cct, 1) << "pool " << it->second << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      lderr(cct) << "error accessing child image pool " << it->second
                 << dendl;
      return r;
    }

    set<string> image_ids;
    r = cls_client::get_children(&ioctx, RBD_CHILDREN, parent_spec,
                                 image_ids);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error reading list of children from pool " << it->second
      	   << dendl;
      return r;
    }
    pool_image_ids->insert({*it, image_ids});
  }

  return 0;
}

template <typename I>
int Image<I>::status_get_version(librados::IoCtx &io_ctx,
    uint64_t *version) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "status_get_version io_ctx=" << &io_ctx << dendl;

  *version = 0;

  int r = cls_client::status_get_version(&io_ctx, RBD_STATUS, version);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error get status version: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Image<I>::status_inc_version(librados::IoCtx &io_ctx, uint64_t version) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "status_inc_version io_ctx=" << &io_ctx << dendl;

  int r = cls_client::status_inc_version(&io_ctx, RBD_STATUS, version);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error inc status version: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Image<I>::status_list_images(librados::IoCtx &io_ctx,
    const std::string &start, size_t max,
    std::vector<status_image_t> *images) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "status_list_images io_ctx=" << &io_ctx << dendl;

  int r;
  size_t got = 0;
  size_t left = max;
  size_t max_read = 1024;
  max_read = std::min(max_read, max);
  if (max_read == 0) {
    max_read = 1024;
    left = static_cast<size_t>(-1);
  }

  std::string last_read = start; // image id as key
  do {
    std::vector<cls::rbd::StatusImage> page;
    r = cls_client::status_list_images(&io_ctx, RBD_STATUS,
        last_read, max_read, &page);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error listing images: "
                 << cpp_strerror(r) << dendl;
      return r;
    } else if (r == -ENOENT) {
      break;
    }

    for (auto &it : page) {
      status_image_t image;
      image.state = it.state;
      image.create_timestamp = it.create_timestamp;
      image.parent.pool_id = it.parent.pool_id;
      image.parent.image_id = it.parent.image_id;
      image.parent.snapshot_id = static_cast<uint64_t>(it.parent.snapshot_id);
      image.data_pool_id = it.data_pool_id;
      image.name = it.name;
      image.id = it.id;
      image.order = it.order;
      image.stripe_unit = it.stripe_unit;
      image.stripe_count = it.stripe_count;
      image.size = it.size;
      image.used = it.used;
      image.qos_iops = it.qos_iops;
      image.qos_bps = it.qos_bps;
      image.qos_reservation = it.qos_reservation;
      image.qos_weight = it.qos_weight;
      for (auto &snap_it : it.snapshot_ids) {
        image.snapshot_ids.push_back(snap_it);
      }

      images->push_back(image);
    }
    if (!page.empty()) {
      last_read = page.rbegin()->id;
    }
    got = page.size();

    if (left != static_cast<size_t>(-1)) {
      left -= got;

      if (left == 0) {
        break;
      }
    }
  } while (got == max_read);

  return 0;
}

template <typename I>
int Image<I>::status_list_snapshots(librados::IoCtx &io_ctx,
    uint64_t start, size_t max,
    std::vector<status_snapshot_t> *snapshots) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "status_list_snapshots io_ctx=" << &io_ctx << dendl;

  int r;
  size_t got = 0;
  size_t left = max;
  size_t max_read = 1024;
  max_read = std::min(max_read, max);
  if (max_read == 0) {
    max_read = 1024;
    left = static_cast<size_t>(-1);
  }

  uint64_t last_read = start; // snapshot id as key
  do {
    std::vector<cls::rbd::StatusSnapshot> page;
    r = cls_client::status_list_snapshots(&io_ctx, RBD_STATUS,
        last_read, max_read, &page);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error listing snapshots: "
                 << cpp_strerror(r) << dendl;
      return r;
    } else if (r == -ENOENT) {
      break;
    }

    for (auto &it : page) {
      status_snapshot_t snapshot;
      snapshot.create_timestamp = it.create_timestamp;
      snapshot.namespace_type = static_cast<status_snapshot_namespace_type_t>(
          it.snapshot_namespace.get_namespace_type());
      snapshot.name = it.name;
      snapshot.image_id = it.image_id;
      snapshot.id = it.id;
      snapshot.size = it.size;
      snapshot.used = it.used;
      snapshot.dirty = it.dirty;
      for (auto &clone_it : it.clone_ids) {
        status_clone_id_t clone;
        clone.pool_id = clone_it.pool_id;
        clone.image_id = clone_it.image_id;

        snapshot.clone_ids.push_back(clone);
      }

      snapshots->push_back(snapshot);
    }
    if (!page.empty()) {
      last_read = page.rbegin()->id;
    }
    got = page.size();

    if (left != static_cast<size_t>(-1)) {
      left -= got;

      if (left == 0) {
        break;
      }
    }
  } while (got == max_read);

  return 0;
}

template <typename I>
int Image<I>::status_list_usages(librados::IoCtx &io_ctx,
    const std::string &start, size_t max,
    std::vector<status_usage_t> *usages) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "status_list_usages io_ctx=" << &io_ctx << dendl;

  int r;
  size_t got = 0;
  size_t left = max;
  size_t max_read = 1024;
  max_read = std::min(max_read, max);
  if (max_read == 0) {
    max_read = 1024;
    left = static_cast<size_t>(-1);
  }

  std::string last_read = start; // image id as key
  do {
    std::vector<cls::rbd::StatusUsage> page;
    r = cls_client::status_list_usages(&io_ctx, RBD_STATUS,
        last_read, max_read, &page);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error listing usages: "
                 << cpp_strerror(r) << dendl;
      return r;
    } else if (r == -ENOENT) {
      break;
    }

    for (auto &it : page) {
      status_usage_t usage;
      usage.state = it.state;
      usage.id = it.id;
      usage.size = it.size;
      usage.used = it.used;

      usages->push_back(usage);
    }
    if (!page.empty()) {
      last_read = page.rbegin()->id;
    }
    got = page.size();

    if (left != static_cast<size_t>(-1)) {
      left -= got;

      if (left == 0) {
        break;
      }
    }
  } while (got == max_read);

  return 0;
}

template <typename I>
int Image<I>::status_get_usage(I *ictx, status_usage_t *usage) {
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << "status_get_usage image_ctx=" << ictx << dendl;

  uint64_t snapshot_id;
  {
    RWLock::RLocker snap_locker(ictx->snap_lock);
    snapshot_id = ictx->snap_id;
  }

  cls::rbd::StatusUsage clsUsage;
  int r = cls_client::status_get_usage(&ictx->md_ctx, RBD_STATUS,
      ictx->id, snapshot_id, &clsUsage);
  if (r < 0) {
    lderr(cct) << "error getting image usage: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  usage->state = clsUsage.state;
  usage->id.clear();
  usage->size = clsUsage.size;
  usage->used = clsUsage.used;

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Image<librbd::ImageCtx>;
