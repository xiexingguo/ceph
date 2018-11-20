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

namespace rbd {

namespace action {
namespace status_list_images {

namespace at = argument_types;
namespace po = boost::program_options;

int do_list(std::string &pool_name, Formatter *f) {
  std::vector<librbd::status_image_t> images;
  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx ioctx;

  int r = utils::init(pool_name, &rados, &ioctx);
  if (r < 0) {
    return r;
  }

  r = rbd.status_list_images(ioctx, "", 0, &images);
  if (r < 0)
    return r;

  if (f) {
    f->open_array_section("images");
  }

  if (!images.empty()) {
    for (auto &it : images) {
      if (f) {
        f->open_object_section("status");

        f->dump_unsigned("state", static_cast<uint64_t>(it.state));

        string ts_str = "";
        ts_str = ctime(&it.create_timestamp);
        ts_str = ts_str.substr(0, ts_str.length() - 1);
        f->dump_string("create_timestamp", ts_str);

        if (!(it.parent.pool_id < 0)) {
          f->open_object_section("parent");

          f->dump_int("pool_id", it.parent.pool_id);
          f->dump_string("image_id", it.parent.image_id);
          f->dump_unsigned("snapshot_id", it.parent.snapshot_id);

          f->close_section();
        }

        if (!(it.data_pool_id < 0)) {
          f->dump_int("data_pool_id", it.data_pool_id);
        }

        f->dump_string("name", it.name);
        f->dump_string("id", it.id);
        f->dump_int("order", it.order);

        if (it.stripe_unit != 0) {
          f->open_object_section("striping");

          f->dump_unsigned("stripe_unit", it.stripe_unit);
          f->dump_unsigned("stripe_count", it.stripe_count);

          f->close_section();
        }

        f->dump_unsigned("size", it.size);
        f->dump_unsigned("used", it.used);

        f->dump_int("qos_iops", it.qos_iops);
        f->dump_int("qos_bps", it.qos_bps);
        f->dump_int("qos_reservation", it.qos_reservation);
        f->dump_int("qos_weight", it.qos_weight);

        if (!it.snapshot_ids.empty()) {
          f->open_array_section("snapshots");

          for (auto &snap_it : it.snapshot_ids) {
            f->dump_unsigned("id", snap_it);
          }

          f->close_section();
        }

        f->close_section();
      }
    }
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  }

  return r < 0 ? r : 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_pool_options(positional, options);
  at::add_format_options(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  namespace at=rbd::argument_types;
  at::Format format("json");
  auto formatter = format.create_formatter(true);

  int r = do_list(pool_name, formatter.get());
  if (r < 0) {
    std::cerr << "rbd: status_list_images: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

Shell::Action action(
  {"list-images"}, {}, "List rbd images.", "", &get_arguments, &execute);

} // namespace status_list_images
} // namespace action
} // namespace rbd
