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
  send_qos_set();
}

template <typename I>
bool QosSetRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << " r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void QosSetRequest<I>::send_qos_set() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

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
  send_qos_remove();
}

template <typename I>
bool QosRemoveRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << " r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void QosRemoveRequest<I>::send_qos_remove() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

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
