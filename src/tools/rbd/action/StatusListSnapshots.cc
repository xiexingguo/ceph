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
namespace status_list_snapshots {

namespace at = argument_types;
namespace po = boost::program_options;

int do_list(std::string &pool_name, Formatter *f) {
  std::vector<librbd::status_snapshot_t> snapshots;
  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx ioctx;

  int r = utils::init(pool_name, &rados, &ioctx);
  if (r < 0) {
    return r;
  }

  r = rbd.status_list_snapshots(ioctx, 0, 0, &snapshots);
  if (r < 0)
    return r;

  if (f) {
    f->open_array_section("snapshots");
  }

  if (!snapshots.empty()) {
    for (auto &it : snapshots) {
      if (f) {
        f->open_object_section("snapshot");

        string ts_str = "";
        ts_str = ctime(&it.create_timestamp);
        ts_str = ts_str.substr(0, ts_str.length() - 1);
        f->dump_string("create_timestamp", ts_str);

        f->dump_unsigned("namespace_type", static_cast<uint64_t>(it.namespace_type));
        f->dump_string("name", it.name);
        f->dump_string("image_id", it.image_id);
        f->dump_unsigned("id", it.id);
        f->dump_unsigned("size", it.size);
        f->dump_unsigned("used", it.used);
        f->dump_unsigned("dirty", it.dirty);

        if (!it.clone_ids.empty()) {
          f->open_array_section("clone_ids");

          for (auto &clone_it : it.clone_ids) {
            f->open_object_section("clone_id");

            f->dump_int("pool_id", clone_it.pool_id);
            f->dump_string("image_id", clone_it.image_id);

            f->close_section();
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
    std::cerr << "rbd: status_list_snapshots: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

Shell::Action action(
  {"list-snapshots"}, {}, "List rbd snapshots.", "", &get_arguments, &execute);

} // namespace status_list_snapshots
} // namespace action
} // namespace rbd
