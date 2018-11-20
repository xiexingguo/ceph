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
namespace status_get_version {

namespace at = argument_types;
namespace po = boost::program_options;

int do_get(std::string &pool_name, Formatter *f) {
  uint64_t version;
  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx ioctx;

  int r = utils::init(pool_name, &rados, &ioctx);
  if (r < 0) {
    return r;
  }

  r = rbd.status_get_version(ioctx, &version);
  if (r < 0)
    return r;

  if (f) {
    f->open_object_section("version");

    f->dump_unsigned("version", version);

    f->close_section();
  }

  if (f) {
    f->flush(std::cout);
  }

  return r < 0 ? r : 0;
}

int do_inc(std::string &pool_name, Formatter *f) {
  uint64_t version;
  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx ioctx;

  int r = utils::init(pool_name, &rados, &ioctx);
  if (r < 0) {
    return r;
  }

  r = rbd.status_inc_version(ioctx, 1);
  if (r < 0)
    return r;

  r = rbd.status_get_version(ioctx, &version);
  if (r < 0)
    return r;

  if (f) {
    f->open_object_section("version");

    f->dump_unsigned("version", version);

    f->close_section();
  }

  if (f) {
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

  int r = do_get(pool_name, formatter.get());
  if (r < 0) {
    std::cerr << "rbd: status_get_version: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int execute_inc(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  namespace at=rbd::argument_types;
  at::Format format("json");
  auto formatter = format.create_formatter(true);

  int r = do_inc(pool_name, formatter.get());
  if (r < 0) {
    std::cerr << "rbd: status_inc_version: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

Shell::Action action(
  {"get-version"}, {}, "Get rbd status version.", "", &get_arguments, &execute);
Shell::Action action2(
  {"inc-version"}, {}, "Increase rbd status version.", "", &get_arguments, &execute_inc);

} // namespace status_get_version
} // namespace action
} // namespace rbd
