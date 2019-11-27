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
namespace status_get_snapshot {

namespace at = argument_types;
namespace po = boost::program_options;

int do_get_usage(librbd::Image& image, Formatter *f) {
  librbd::status_snapshot_t snap;

  int r = image.status_get_snapshot(&snap);
  if (r < 0)
    return r;

  if (f) {
    f->open_object_section("snapshot");

    string ts_str = "";
    ts_str = ctime(&snap.create_timestamp);
    ts_str = ts_str.substr(0, ts_str.length() - 1);
    f->dump_string("create_timestamp", ts_str);

    f->dump_unsigned("namespace_type", static_cast<uint64_t>(snap.namespace_type));
    f->dump_string("name", snap.name);
    f->dump_string("image_id", snap.image_id);
    f->dump_unsigned("id", snap.id);
    f->dump_unsigned("size", snap.size);
    f->dump_unsigned("used", snap.used);
    f->dump_unsigned("dirty", snap.dirty);

    if (!snap.clone_ids.empty()) {
      f->open_array_section("clone_ids");

      for (auto &clone_it : snap.clone_ids) {
        f->open_object_section("clone_id");

        f->dump_int("pool_id", clone_it.pool_id);
        f->dump_string("image_id", clone_it.image_id);

        f->close_section();
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
  at::add_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
      &snap_name, utils::SNAPSHOT_PRESENCE_REQUIRED, utils::SPEC_VALIDATION_NONE);
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

  r = do_get_usage(image, formatter.get());
  if (r < 0) {
    std::cerr << "rbd: get_snapshot: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

Shell::Action action(
  {"status-get-snapshot"}, {}, "Get rbd snapshot.", "", &get_arguments, &execute);

}// namespace status_get_snapshot
} // namespace action
} // namespace rbd
