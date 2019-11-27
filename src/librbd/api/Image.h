// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_IMAGE_H
#define LIBRBD_API_IMAGE_H

#include "include/rbd/librbd.hpp"
#include "librbd/Types.h"
#include "include/rbd/librbd.hpp"
#include <map>
#include <set>
#include <string>
#include <vector>

namespace librados { struct IoCtx; }

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct Image {
  typedef std::pair<int64_t, std::string> PoolSpec;
  typedef std::set<std::string> ImageIds;
  typedef std::map<PoolSpec, ImageIds> PoolImageIds;
  typedef std::map<std::string, std::string> ImageNameToIds;

  static int list_images(librados::IoCtx& io_ctx,
                         std::vector<image_spec_t> *images);
  static int list_images_v2(librados::IoCtx& io_ctx,
                            ImageNameToIds *images);

  static int list_children(ImageCtxT *ictx, const ParentSpec &parent_spec,
                           PoolImageIds *pool_image_ids);

  static int status_get_version(librados::IoCtx &io_ctx, uint64_t *version);
  static int status_inc_version(librados::IoCtx &io_ctx, uint64_t version);
  static int status_set_version(librados::IoCtx &io_ctx, uint64_t version);

  static int status_list_images(librados::IoCtx &io_ctx,
      const std::string &start, size_t max,
      std::vector<status_image_t> *statuses);
  static int status_list_snapshots(librados::IoCtx &io_ctx,
      uint64_t start, size_t max,
      std::vector<status_snapshot_t> *snapshots);
  static int status_list_usages(librados::IoCtx &io_ctx,
      const std::string &start, size_t max,
      std::vector<status_usage_t> *usages);

  static int status_get_image(ImageCtxT *ictx, status_image_t *image);
  static int status_get_snapshot(ImageCtxT *ictx, status_snapshot_t *snap);
  static int status_get_usage(ImageCtxT *ictx, status_usage_t *usage);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::Image<librbd::ImageCtx>;

#endif // LIBRBD_API_IMAGE_H
