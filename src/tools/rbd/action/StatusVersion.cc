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

int do_inc(std::string &pool_name, Formatter *f, int64_t version) {
  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx ioctx;

  int r = utils::init(pool_name, &rados, &ioctx);
  if (r < 0) {
    return r;
  }

  r = rbd.status_inc_version(ioctx, version);
  if (r < 0)
    return r;

  uint64_t ver;
  r = rbd.status_get_version(ioctx, &ver);
  if (r < 0)
    return r;

  if (f) {
    f->open_object_section("version");

    f->dump_unsigned("version", ver);

    f->close_section();
  }

  if (f) {
    f->flush(std::cout);
  }

  return r < 0 ? r : 0;
}

int do_set(std::string &pool_name, Formatter *f, uint64_t version) {
  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx ioctx;

  int r = utils::init(pool_name, &rados, &ioctx);
  if (r < 0) {
    return r;
  }

  r = rbd.status_set_version(ioctx, version);
  if (r < 0)
    return r;

  uint64_t ver;
  r = rbd.status_get_version(ioctx, &ver);
  if (r < 0)
    return r;

  if (f) {
    f->open_object_section("version");

    f->dump_unsigned("version", ver);

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

void get_inc_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_pool_options(positional, options);
  at::add_format_options(options);
  positional->add_options()
      ("version", "version to increase");
}

void get_set_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_pool_options(positional, options);
  at::add_format_options(options);
  positional->add_options()
      ("version", "version to set");
}

int get_inc_version(const po::variables_map &vm, size_t *arg_index, int64_t *version) {
  std::string verstr = utils::get_positional_argument(vm, *arg_index);
  if (!verstr.empty()) {
     ++(*arg_index);
  } else {
    verstr = "1";
  }

  char *end;
  int64_t ver = std::strtoll(verstr.c_str(), &end, 10);
  if (*end != '\0') {
    return -EINVAL;
  }
  *version = ver;
  return 0;
}

int get_set_version(const po::variables_map &vm, size_t *arg_index, uint64_t *version) {
  std::string verstr = utils::get_positional_argument(vm, *arg_index);
  if (!verstr.empty()) {
     ++(*arg_index);
  } else {
    return -EINVAL;
  }

  // https://wiki.sei.cmu.edu/confluence/display/c/ERR34-C.+Detect+errors+when+converting+a+string+to+a+number
  char *end;
  int64_t ver = std::strtoull(verstr.c_str(), &end, 10);
  if (*end != '\0') {
    return -EINVAL;
  }
  *version = ver;
  return 0;
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
  int64_t version;
  int r = get_inc_version(vm, &arg_index, &version);
  if (r < 0) {
    std::cerr << "rbd: status_inc_version: " << cpp_strerror(r) << std::endl;
    return r;
  }

  namespace at=rbd::argument_types;
  at::Format format("json");
  auto formatter = format.create_formatter(true);

  r = do_inc(pool_name, formatter.get(), version);
  if (r < 0) {
    std::cerr << "rbd: status_inc_version: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int execute_set(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);
  uint64_t version;
  int r = get_set_version(vm, &arg_index, &version);
  if (r < 0) {
    std::cerr << "rbd: status_set_version: " << cpp_strerror(r) << std::endl;
    return r;
  }

  namespace at=rbd::argument_types;
  at::Format format("json");
  auto formatter = format.create_formatter(true);

  r = do_set(pool_name, formatter.get(), version);
  if (r < 0) {
    std::cerr << "rbd: status_set_version: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

Shell::Action action(
  {"status-get-version"}, {}, "Get rbd status version.", "", &get_arguments, &execute);
Shell::Action action2(
  {"status-inc-version"}, {}, "Increase rbd status version.", "", &get_inc_arguments, &execute_inc);
Shell::Action action3(
  {"status-set-version"}, {}, "Set rbd status version.", "", &get_set_arguments, &execute_set);

} // namespace status_get_version
} // namespace action
} // namespace rbd
