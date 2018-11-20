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
namespace status_list_usages {

namespace at = argument_types;
namespace po = boost::program_options;

int do_list(std::string &pool_name, Formatter *f) {
  std::vector<librbd::status_usage_t> usages;
  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx ioctx;

  int r = utils::init(pool_name, &rados, &ioctx);
  if (r < 0) {
    return r;
  }

  r = rbd.status_list_usages(ioctx, "", 0, &usages);
  if (r < 0)
    return r;

  if (f) {
    f->open_array_section("usages");
  }

  if (!usages.empty()) {
    for (auto &it : usages) {
      if (f) {
        f->open_object_section("usage");

        f->dump_unsigned("state", it.state);
        f->dump_string("id", it.id);
        f->dump_unsigned("size", it.size);
        f->dump_unsigned("used", it.used);

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
    std::cerr << "rbd: status_list_usages: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

Shell::Action action(
  {"list-usages"}, {}, "List rbd usages.", "", &get_arguments, &execute);

} // namespace status_list_usages
} // namespace action
} // namespace rbd
