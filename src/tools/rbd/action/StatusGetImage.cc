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
namespace status_get_image {

namespace at = argument_types;
namespace po = boost::program_options;

int do_get_image(librbd::Image& image_, Formatter *f) {
  librbd::status_image_t image;

  int r = image_.status_get_image(&image);
  if (r < 0)
    return r;

  if (f) {
    f->open_object_section("image");

    f->dump_unsigned("state", static_cast<uint64_t>(image.state));

    string ts_str = "";
    ts_str = ctime(&image.create_timestamp);
    ts_str = ts_str.substr(0, ts_str.length() - 1);
    f->dump_string("create_timestamp", ts_str);

    if (!(image.parent.pool_id < 0)) {
      f->open_object_section("parent");

      f->dump_int("pool_id", image.parent.pool_id);
      f->dump_string("image_id", image.parent.image_id);
      f->dump_unsigned("snapshot_id", image.parent.snapshot_id);

      f->close_section();
    }

    if (!(image.data_pool_id < 0)) {
      f->dump_int("data_pool_id", image.data_pool_id);
    }

    f->dump_string("name", image.name);
    f->dump_string("id", image.id);
    f->dump_int("order", image.order);

    if (image.stripe_unit != 0) {
      f->open_object_section("striping");

      f->dump_unsigned("stripe_unit", image.stripe_unit);
      f->dump_unsigned("stripe_count", image.stripe_count);

      f->close_section();
    }

    f->dump_unsigned("size", image.size);
    f->dump_unsigned("used", image.used);

    f->dump_int("qos_iops", image.qos_iops);
    f->dump_int("qos_bps", image.qos_bps);
    // both reservation and weight are ignored
//    f->dump_int("qos_reservation", image.qos_reservation);
//    f->dump_int("qos_weight", image.qos_weight);

    if (!image.snapshot_ids.empty()) {
      f->open_array_section("snapshots");

      for (auto &snap_it : image.snapshot_ids) {
        f->dump_unsigned("id", snap_it);
      }

      f->close_section();
    }

    f->close_section();
  }

  if (f) {
    f->flush(std::cout);
  }

  return r < 0 ? r : 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
      &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  namespace at=rbd::argument_types;
  at::Format format("json");
  auto formatter = format.create_formatter(true);

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", "", true,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_get_image(image, formatter.get());
  if (r < 0) {
    std::cerr << "rbd: get_image: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

Shell::Action action(
  {"status-get-image"}, {}, "Get rbd image.", "", &get_arguments, &execute);

}// namespace status_get_image
} // namespace action
} // namespace rbd
