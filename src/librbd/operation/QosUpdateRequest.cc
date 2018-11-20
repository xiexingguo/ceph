// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/QosUpdateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::QosUpdateRequest: "

namespace librbd {
namespace operation {

template <typename I>
QosSetRequest<I>::QosSetRequest(I &image_ctx,
                                Context *on_finish)
  : Request<I>(image_ctx, on_finish) {
}

template <typename I>
void QosSetRequest<I>::send_op() {
  send_status_update();
}

template <typename I>
bool QosSetRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << " r=" << r << dendl;

  if (m_state == STATE_STATUS_UPDATE) {
    if (r == -EOPNOTSUPP || r == -ENOENT) {
      r = 0;
    }
  }

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }

  RWLock::RLocker owner_locker(image_ctx.owner_lock);
  bool finished = false;
  switch (m_state) {
  case STATE_STATUS_UPDATE:
    ldout(cct, 5) << "STATUS_UPDATE" << dendl;
    send_qos_set();
    break;
  case STATE_UPDATE_METADATA:
    ldout(cct, 5) << "UPDATE_METADATA" << dendl;
    finished = true;
    break;
  default:
    lderr(cct) << "invalid state: " << m_state << dendl;
    assert(false);
    break;
  }
  return finished;
}

template <typename I>
void QosSetRequest<I>::send_status_update() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  m_state = STATE_STATUS_UPDATE;

  int iops = -2, bps = -2, reservation = -2, weight = -2;
  for (const auto& i : m_data) {
    auto k = i.first;
    std::string v = i.second.to_str();

    if (k == QOS_MLMT) {
      iops = std::stoi(v);
    }
    if (k == QOS_MBDW) {
      bps = std::stoi(v);
    }
    if (k == QOS_MRSV) {
      reservation = std::stoi(v);
    }
    if (k == QOS_MWGT) {
      weight = std::stoi(v);
    }
  }

  if (iops == -2 && bps == -2 && reservation == -2 && weight == -2) {
    send_qos_set();
    return;
  }

  librados::ObjectWriteOperation op;
  cls_client::status_update_qos(&op, image_ctx.id, iops, bps,
      reservation, weight);

  librados::AioCompletion *comp = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(RBD_STATUS, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
void QosSetRequest<I>::send_qos_set() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  m_state = STATE_UPDATE_METADATA;

  librados::ObjectWriteOperation op;
  cls_client::metadata_set(&op, m_data);

  librados::AioCompletion *comp = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
void QosSetRequest<I>::add_qos_keyval(const std::string &key,
                                      const std::string &value) {
  m_data[key].append(value);
}

template <typename I>
QosRemoveRequest<I>::QosRemoveRequest(I &image_ctx,
                                Context *on_finish)
  : Request<I>(image_ctx, on_finish) {
}

template <typename I>
void QosRemoveRequest<I>::send_op() {
  send_status_update();
}

template <typename I>
bool QosRemoveRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << " r=" << r << dendl;

  if (m_state == STATE_STATUS_UPDATE) {
    if (r == -EOPNOTSUPP || r == -ENOENT) {
      r = 0;
    }
  }

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }

  RWLock::RLocker owner_locker(image_ctx.owner_lock);
  bool finished = false;
  switch (m_state) {
  case STATE_STATUS_UPDATE:
    ldout(cct, 5) << "STATUS_UPDATE" << dendl;
    send_qos_remove();
    break;
  case STATE_UPDATE_METADATA:
    ldout(cct, 5) << "UPDATE_METADATA" << dendl;
    finished = true;
    break;
  default:
    lderr(cct) << "invalid state: " << m_state << dendl;
    assert(false);
    break;
  }
  return finished;
}

template <typename I>
void QosRemoveRequest<I>::send_status_update() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  m_state = STATE_STATUS_UPDATE;

  int iops = -2, bps = -2, reservation = -2, weight = -2;
  for (const auto& i : m_data) {
    auto k = i;

    if (k == QOS_MLMT) {
      iops = -1;
    }
    if (k == QOS_MBDW) {
      bps = -1;
    }
    if (k == QOS_MRSV) {
      reservation = -1;
    }
    if (k == QOS_MWGT) {
      weight = -1;
    }
  }

  if (iops == -2 && bps == -2 && reservation == -2 && weight == -2) {
    send_qos_remove();
    return;
  }

  librados::ObjectWriteOperation op;
  cls_client::status_update_qos(&op, image_ctx.id, iops, bps,
      reservation, weight);

  librados::AioCompletion *comp = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(RBD_STATUS, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
void QosRemoveRequest<I>::send_qos_remove() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  m_state = STATE_UPDATE_METADATA;

  librados::ObjectWriteOperation op;
  for (auto & key : m_data) {
    cls_client::metadata_remove(&op, key);
  }

  librados::AioCompletion *comp = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
void QosRemoveRequest<I>::add_qos_key(const std::string &key) {
  m_data.insert(key);
}

} // namespace operation
} // namespace librbd

template class librbd::operation::QosSetRequest<librbd::ImageCtx>;
template class librbd::operation::QosRemoveRequest<librbd::ImageCtx>;
