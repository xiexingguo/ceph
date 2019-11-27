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
namespace status_get_usage {

namespace at = argument_types;
namespace po = boost::program_options;

int do_get_usage(librbd::Image& image, bool isSnap, Formatter *f) {
  librbd::status_usage_t usage;

  int r = image.status_get_usage(&usage);
  if (r < 0)
    return r;

  if (f) {
    f->open_object_section("usage");

    f->dump_string("id", usage.id);
    if (isSnap) {
      f->dump_unsigned("dirty", usage.state);
    } else {
      f->dump_unsigned("state", usage.state);
    }
    f->dump_unsigned("size", usage.size);
    f->dump_unsigned("used", usage.used);

    f->close_section();
  }

  if (f) {
    f->flush(std::cout);
  }

  return r < 0 ? r : 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
      &snap_name, utils::SNAPSHOT_PRESENCE_PERMITTED, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  namespace at=rbd::argument_types;
  at::Format format("json");
  auto formatter = format.create_formatter(true);

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", snap_name, true,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_get_usage(image, !snap_name.empty(), formatter.get());
  if (r < 0) {
    std::cerr << "rbd: get_usage: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

Shell::Action action(
  {"status-get-usage"}, {}, "Get rbd usage.", "", &get_arguments, &execute);

}// namespace status_get_usage
} // namespace action
} // namespace rbd
