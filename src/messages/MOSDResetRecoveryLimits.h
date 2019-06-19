// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 ZTE Corporation
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_MOSDRESETRECOVERYLIMITS_H
#define CEPH_MOSDRESETRECOVERYLIMITS_H

#include "msg/Message.h"

/*
 * instruct an OSD to boost/unboost recovery/backfill priority
 */
#define OSD_RESET_RECOVERY_BANDWIDTH    (1<<0)
#define OSD_RESET_RECOVERY_MAXACTIVE    (1<<1)
#define OSD_RESET_MAX_BACKFILLS         (1<<2)

struct MOSDResetRecoveryLimits : public Message {
  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;

  uuid_d  fsid;
  uint8_t options;
  double bandwidth_factor;
  double maxactive_factor;
  double aggressive_factor;
  double max_backfills_factor;

  MOSDResetRecoveryLimits() :
    Message(MSG_OSD_RESET_RECOVERY_LIMITS, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDResetRecoveryLimits(
    const uuid_d& f,
    uint8_t ops,
    double max_backfills,
    double bandwidth,
    double maxactive,
    double aggressive = 1.0) :
    Message(MSG_OSD_RESET_RECOVERY_LIMITS, HEAD_VERSION, COMPAT_VERSION),
    fsid(f),
    options(ops),
    bandwidth_factor(bandwidth),
    maxactive_factor(maxactive),
    aggressive_factor(aggressive),
    max_backfills_factor(max_backfills) {}
private:
  ~MOSDResetRecoveryLimits() {}

public:
  const char *get_type_name() const { return "reset_recovery_limits"; }
  void print(ostream& out) const {
    out << "reset_recovery_limits(";
    out << "options=" << options;
    out << " bandwidth_factor=" << bandwidth_factor;
    out << " maxactive_factor=" << maxactive_factor;
    out << " aggressive_factor=" << aggressive_factor;
    out << " max_backfills_factor=" << max_backfills_factor;
    out << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(fsid, payload);
    ::encode(options, payload);
    ::encode(bandwidth_factor, payload);
    ::encode(maxactive_factor, payload);
    ::encode(aggressive_factor, payload);
    // v2 for backfills factor
    ::encode(max_backfills_factor, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(options, p);
    ::decode(bandwidth_factor, p);
    ::decode(maxactive_factor, p);
    ::decode(aggressive_factor, p);
    if (header.version >= 2)
      ::decode(max_backfills_factor, p);
  }
};

#endif /* CEPH_MOSDRESETRECOVERYLIMITS_H */
