// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#pragma once

#include <ostream>

#include "boost/variant.hpp"
#include "boost/regex.hpp"

#include "common/config.h"
#include "common/ceph_context.h"
#include "osd/PGQueueable.h"

#include "common/mClockPriorityQueue.h"
#include "messages/MOSDOp.h"
#include "common/dout.h"

namespace ceph {

  using Request = std::pair<spg_t, PGQueueable>;
  using Client = entity_inst_t;

  // This class exists to bridge the ceph code, which treats the class
  // as the client, and the queue, where the class is
  // osd_op_type_t. So this adpater class will transform calls
  // appropriately.
  class dmcQueue : public OpQueue<Request, Client> {
    CephContext *cct;
public:
    enum class osd_op_type_t {
      client_op, osd_subop, osd_subop_pullpush, bg_snaptrim, bg_recovery, bg_scrub };

    using dmcClient = std::pair<entity_inst_t, osd_op_type_t>;

    using queue_t = mClockQueue<Request, dmcClient>;

    queue_t queue;

    map<osd_op_type_t, crimson::dmclock::ClientInfo> global_dmc_qos_map;

    bool parse_qos_spec_config(string svalue,
                               double& rsv, double& wgt, double& lmt, double& bdw); 
    void load_global_dmc_qos_config();
    void update_config(const std::string sitem, std::string svalue, bool init) override final;
    dmc_op_tracker get_dmc_op_tracker(Request &item);
    dmcClient make_dmcclient(Client client, Request item);
    dmc::ClientInfo fill_in_client_info(Request item);
    dmc::ReqParams fill_in_req_params(dmc_op_tracker opt);
    static dmc::ClientInfo client_info_func(const dmcClient& client);

  protected:

    struct pg_queueable_visitor : public boost::static_visitor<osd_op_type_t> {
      osd_op_type_t operator()(const OpRequestRef& o) const {
        return osd_op_type_t::client_op;
      }
      osd_op_type_t operator()(const PGSnapTrim& o) const {
        return osd_op_type_t::bg_snaptrim;
      }
      osd_op_type_t operator()(const PGRecovery& o) const {
        return osd_op_type_t::bg_recovery;
      }
      osd_op_type_t operator()(const PGScrub& o) const {
        return osd_op_type_t::bg_scrub;
      }
    };

    static pg_queueable_visitor visitor;

    osd_op_type_t get_osd_op_type(const Request& request);
    void set_request_phase(const dmc::PhaseType phase, Request &item);

  public:

    dmcQueue(CephContext *_cct);
    inline unsigned length() const override final {
      return queue.length();
    }

    // ops of this priority should be deleted immediately
    inline void remove_by_class(Client client, std::list<Request> *out)
      override final {
      queue.remove_by_filter(
      [&client, out] (const Request& r) -> bool {
         if (client == r.second.get_owner()) {
           out->push_front(r);
           return true;
         } else {
           return false;
         }
       });
    }

    inline void enqueue_strict(Client client, unsigned priority, Request item)
      override final {
      queue.enqueue_strict(make_dmcclient(client, item), 0, item);
    }

    // enqueue op in the front of the strict queue
    inline void enqueue_strict_front(Client client, unsigned priority, Request item)
      override final {
      queue.enqueue_strict_front(make_dmcclient(client, item), priority, item);
    }

    // enqueue the op in the front of the regular queue
    inline void enqueue_front(Client client, unsigned priority, unsigned cost,
      Request item) override final {
      queue.enqueue_front(make_dmcclient(client, item), priority, cost, item);
    }

    // enqueue op in the back of the regular queue
    inline void enqueue(Client client, unsigned priority, unsigned cost,
      Request item) override final {
      // cost is ignored
      queue.enqueue_dmc(make_dmcclient(client, item), priority, 0, item,
        fill_in_client_info(item),
        fill_in_req_params(get_dmc_op_tracker(item)));
    }

    // return if the queue is empty
    inline bool empty() const override final {
      return queue.empty();
    }

    // return an op to be dispatched
    inline Request dequeue() override final {
      bool from_dmc;
      dmc::PhaseType phase;
      Request r = queue.dequeue_dmc(&from_dmc, &phase);
      if (from_dmc) // only set phase if request comes out from a dmc queue
        set_request_phase(phase, r);
      return r;
    }

    // rormatted output of the queue
    void dump(ceph::Formatter *f) const override final;
  }; // class dmcQueue

  inline ostream& operator<<(ostream& out,
    const dmcQueue::dmcClient& client) {
    switch(client.second) {
    case dmcQueue::osd_op_type_t::client_op:
      out << "<clientop>";
      break;
    case dmcQueue::osd_op_type_t::osd_subop:
      out << "<subop   >";
      break;
    case dmcQueue::osd_op_type_t::osd_subop_pullpush:
      out << "<pullpush>";
      break;
    case dmcQueue::osd_op_type_t::bg_snaptrim:
      out << "<snaptrim>";
      break;
    case dmcQueue::osd_op_type_t::bg_recovery:
      out << "<recovery>";
      break;
    case dmcQueue::osd_op_type_t::bg_scrub:
      out << "<scrub   >";
      break;
    default:
      out << "<\?\?\?>";
    }
    return out << " " << client.first;
  }

} // namespace ceph

