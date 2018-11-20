// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_QOS_UPDATE_REQUEST_H
#define CEPH_LIBRBD_OPERATION_QOS_UPDATE_REQUEST_H

#include "librbd/operation/Request.h"
#include "include/buffer.h"
#include <string>
#include <map>

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class QosSetRequest : public Request<ImageCtxT> {
public:
  QosSetRequest(ImageCtxT &image_ctx, Context *on_finish);
  void add_qos_keyval(const std::string &key,
                      const std::string &value);

protected:
  void send_op() override;
  bool should_complete(int r) override;

  journal::Event create_event(uint64_t op_tid) const override {
    return journal::MetadataSetEvent();
  }

private:
  enum State {
    STATE_STATUS_UPDATE,
    STATE_UPDATE_METADATA
  };
  State m_state;
  std::map<std::string, bufferlist> m_data;

  void send_status_update();
  void send_qos_set();
};

template <typename ImageCtxT = ImageCtx>
class QosRemoveRequest : public Request<ImageCtxT> {
public:
  QosRemoveRequest(ImageCtxT &image_ctx, Context *on_finish);
  void add_qos_key(const std::string &key);

protected:
  void send_op() override;
  bool should_complete(int r) override;

  journal::Event create_event(uint64_t op_tid) const override {
    return journal::MetadataSetEvent();
  }

private:
  enum State {
    STATE_STATUS_UPDATE,
    STATE_UPDATE_METADATA
  };
  State m_state;
  std::set<std::string> m_data;

  void send_status_update();
  void send_qos_remove();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::QosSetRequest<librbd::ImageCtx>;
extern template class librbd::operation::QosRemoveRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_QOS_UPDATE_REQUEST_H

