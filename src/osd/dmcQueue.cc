#include <memory>

#include "osd/dmcQueue.h"
#include "common/dout.h"

namespace dmc = crimson::dmclock;

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "dmcQueue " << __func__ << " "

namespace ceph {

  dmcQueue::dmcQueue(CephContext *_cct) : cct(_cct), queue(&client_info_func, false) {
    load_global_dmc_qos_config();
  }

  dmcQueue::pg_queueable_visitor
  dmcQueue::visitor;

  bool dmcQueue::parse_qos_spec_config(string svalue,
                             double& rsv, double& wgt, double& lmt, double& bdw) {
    char unit = '\0';
    boost::regex spec_pattern("([0-9]+\\.?[0-9]*,){3}[0-9]+\\.?[0-9]*[KMGT]?$");
    if (!boost::regex_match(svalue, spec_pattern)) {
      ldout(cct, 0) << "invalid configuration: " << svalue << ", "
                    << "pattern mismatch: " << spec_pattern << dendl;
      return false;
    }

    int r = sscanf(svalue.c_str(), "%lf,%lf,%lf,%lf%c",
                   &rsv, &wgt, &lmt, &bdw, &unit);
    if (r < 4 || r > 5) {
      ldout(cct, 0) << "invalid configuration: " << svalue << ", "
                    << "sscanf return " << r << dendl;
      return false;
    }

    switch(unit) {
    case 'T': bdw *= 1024; // fall through
    case 'G': bdw *= 1024; // fall through
    case 'M': bdw *= 1024; // fall through
    case 'K': bdw *= 1024; // fall through
    default: break;
    }
    return true;
  }

  void dmcQueue::load_global_dmc_qos_config() {
    md_config_t *conf = cct->_conf;
    update_config("osd_dmc_queue_spec_clientop", conf->osd_dmc_queue_spec_clientop, true);
    update_config("osd_dmc_queue_spec_subop", conf->osd_dmc_queue_spec_subop, true);
    update_config("osd_dmc_queue_spec_pullpush", conf->osd_dmc_queue_spec_pullpush, true);
    update_config("osd_dmc_queue_spec_snaptrim", conf->osd_dmc_queue_spec_snaptrim, true);
    update_config("osd_dmc_queue_spec_recovery", conf->osd_dmc_queue_spec_recovery, true);
    update_config("osd_dmc_queue_spec_scrub", conf->osd_dmc_queue_spec_scrub, true);
  }

  void dmcQueue::update_config(const std::string sitem, std::string svalue, bool init) {
    crimson::dmclock::ClientInfo qos;
    if (!parse_qos_spec_config(svalue,
        qos.reservation, qos.weight, qos.limit, qos.bandwidth) || !qos.valid()) {
      ldout(cct, 0) << "parse config failed or invalid qos spec: ["
                    << qos.reservation << ","
                    << qos.weight << "," << qos.limit << ","
                    << qos.bandwidth << "]" << dendl;
      if (!init) {
        return;
      }
      crimson::dmclock::ClientInfo qos_def(0, 100, 0, 0);
      qos = qos_def;
    }

    ldout(cct, 20) << "set " << sitem <<" to ["
                   << qos.reservation << ","
                   << qos.weight << "," << qos.limit << ","
                   << qos.bandwidth << "]" << dendl;

    if (sitem == "osd_dmc_queue_spec_clientop") {
      global_dmc_qos_map[osd_op_type_t::client_op].assign_spec(qos);
      global_dmc_qos_map[osd_op_type_t::client_op].version++;
    } else if (sitem == "osd_dmc_queue_spec_subop") {
      global_dmc_qos_map[osd_op_type_t::osd_subop].assign_spec(qos);
      global_dmc_qos_map[osd_op_type_t::osd_subop].version++;
    } else if (sitem == "osd_dmc_queue_spec_pullpush") {
      global_dmc_qos_map[osd_op_type_t::osd_subop_pullpush].assign_spec(qos);
      global_dmc_qos_map[osd_op_type_t::osd_subop_pullpush].version++;
    } else if (sitem == "osd_dmc_queue_spec_snaptrim") {
      global_dmc_qos_map[osd_op_type_t::bg_snaptrim].assign_spec(qos);
      global_dmc_qos_map[osd_op_type_t::bg_snaptrim].version++;
    } else if (sitem == "osd_dmc_queue_spec_recovery") {
      global_dmc_qos_map[osd_op_type_t::bg_recovery].assign_spec(qos);
      global_dmc_qos_map[osd_op_type_t::bg_recovery].version++;
    } else if (sitem == "osd_dmc_queue_spec_scrub") {
      global_dmc_qos_map[osd_op_type_t::bg_scrub].assign_spec(qos);
      global_dmc_qos_map[osd_op_type_t::bg_scrub].version++;
    }
  }

  dmc_op_tracker dmcQueue::get_dmc_op_tracker(Request &item) {
    osd_op_type_t type = get_osd_op_type(item);
    if (type == osd_op_type_t::client_op) {
      const OpRequestRef op =
        boost::get<OpRequestRef>(item.second.get_variant());
      assert(op->get_req()->get_type() == CEPH_MSG_OSD_OP);
      MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
      dmc_op_tracker opt = m->get_dmc_op_tracker();
      if (opt.valid())
        return opt;
    } else if (type == osd_op_type_t::osd_subop_pullpush) {
      const OpRequestRef op =
        boost::get<OpRequestRef>(item.second.get_variant());
      dmc_op_tracker opt = op->get_dmc_op_tracker();
      if (opt.valid())
        return opt;
    }

    return dmc_op_tracker(1, 1);
  }

  dmcQueue::dmcClient
  dmcQueue::make_dmcclient(Client client, Request item) {
    osd_op_type_t type = get_osd_op_type(item);
    if (type == osd_op_type_t::osd_subop_pullpush) {
      return std::make_pair(Client(), type);
    }
    return std::make_pair(client, type);
  }

  dmc::ClientInfo dmcQueue::fill_in_client_info(Request item) {
    osd_op_type_t type = get_osd_op_type(item);
    if (type == osd_op_type_t::client_op) {
      /*
       * For client ops,  use client-specified QoS parameters,
       * if there is any.
       */
      const OpRequestRef op =
        boost::get<OpRequestRef>(item.second.get_variant());
      assert(op->get_req()->get_type() == CEPH_MSG_OSD_OP);
      MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
      dmc_qos_spec qos = m->get_dmc_qos_spec();
      if (qos.valid()) {
        return dmc::ClientInfo(qos.reservation, qos.weight,
                               qos.limit, qos.bandwidth, qos.version);
      }
    }

    return global_dmc_qos_map[type];
  }

  dmc::ReqParams dmcQueue::fill_in_req_params(dmc_op_tracker opt) {
    return dmc::ReqParams(opt.delta, opt.rho, opt.cost);
  }

  dmc::ClientInfo dmcQueue::client_info_func(const dmcClient& client) {
    // global client info obtainer, DO NOT USE
    assert(0 == "implement me");
    return dmc::ClientInfo();
  }

  dmcQueue::osd_op_type_t
  dmcQueue::get_osd_op_type(const Request& request) {
    osd_op_type_t type =
      boost::apply_visitor(visitor, request.second.get_variant());

    if (type != osd_op_type_t::client_op)
      return type;

    const OpRequestRef op =
      boost::get<OpRequestRef>(request.second.get_variant());
    uint16_t msg_type = op->get_req()->get_header().type;
    if (msg_type == CEPH_MSG_OSD_OP) {
      return osd_op_type_t::client_op;
    } else if (msg_type == MSG_OSD_PG_PULL || msg_type == MSG_OSD_PG_PUSH) {
      return osd_op_type_t::osd_subop_pullpush;
    } else {
      return osd_op_type_t::osd_subop;
    }

    return type;
  }

  void dmcQueue::set_request_phase(const dmc::PhaseType phase, Request &item) {
    osd_op_type_t type =
      boost::apply_visitor(visitor, item.second.get_variant());

    if (type != osd_op_type_t::client_op)
      return;

    dmc_op_tracker opt;
    switch (phase) {
    case dmc::PhaseType::reservation:
      opt.phase = DMC_OP_PHASE_RESERVATION;
      break;
    case dmc::PhaseType::priority:
      opt.phase = DMC_OP_PHASE_PRIORITY;
      break;
    default:
      assert(0 == "invalid phase type");
      break;
    }
    // set the cost and bring it back to client later.
    // just the write operation take effect, since read cost is zero here.
    opt.cost = item.second.get_cost();

    OpRequestRef op =
      boost::get<OpRequestRef>(item.second.get_variant());
    op->set_dmc_op_tracker(opt);
  }

  void dmcQueue::dump(ceph::Formatter *f) const {
    f->open_object_section("default qos specs");
    for (auto it = global_dmc_qos_map.begin(); it != global_dmc_qos_map.end(); it++) {
      std::stringstream oss;
      if (it->first == osd_op_type_t::client_op) {
        oss << "<clientop>";
      } else if (it->first == osd_op_type_t::osd_subop) {
        oss << "<subop   >";
      } else if (it->first == osd_op_type_t::osd_subop_pullpush) {
        oss << "<pullpush>";
      } else if (it->first == osd_op_type_t::bg_snaptrim) {
        oss << "<snaptrim>";
      } else if (it->first == osd_op_type_t::bg_recovery) {
        oss << "<recovery>";
      } else if (it->first == osd_op_type_t::bg_scrub) {
        oss << "<scrub   >";
      } else {
        assert(0 == "invalid osd op type");
      }
      f->dump_stream(oss.str().c_str())
         << "[" << it->second.reservation << "/" << it->second.reservation_inv
         << "," << it->second.weight << "/" << it->second.weight_inv
         << "," << it->second.limit << "/" << it->second.limit_inv
         << "," << it->second.bandwidth << "/" << it->second.bandwidth_inv
         << "].v" << it->second.version;
    }
    f->close_section();
    queue.dump(f);
  }

}

