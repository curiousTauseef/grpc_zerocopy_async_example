#include "grpc_util.h"

GrpcByteBufferSource::GrpcByteBufferSource() {}

bool GrpcByteBufferSource::Init(const grpc::ByteBuffer& src) {
  cur_ = -1;
  left_ = 0;
  ptr_ = nullptr;
  byte_count_ = 0;
  bool ok = src.Dump(&slices_).ok();
  if (!ok) {
    slices_.clear();
  }
  return ok;
}

bool GrpcByteBufferSource::Next(const void** data, int* size) {
  // Use loop instead of if in case buffer contained empty slices.
  while (left_ == 0) {
    // Advance to next slice.
    cur_++;
    if (cur_ >= slices_.size()) {
      return false;
    }
    const ::grpc::Slice& s = slices_[cur_];
    left_ = s.size();
    ptr_ = reinterpret_cast<const char*>(s.begin());
  }

  *data = ptr_;
  *size = left_;
  byte_count_ += left_;
  ptr_ += left_;
  left_ = 0;
  return true;
}

void GrpcByteBufferSource::BackUp(int count) {
  ptr_ -= count;
  left_ += count;
  byte_count_ -= count;
}

bool GrpcByteBufferSource::Skip(int count) {
  const void* data;
  int size;
  while (Next(&data, &size)) {
    if (size >= count) {
      BackUp(size - count);
      return true;
    }
    // size < count;
    count -= size;
  }
  // error or we have too large count;
  return false;
}

grpc::protobuf::int64 GrpcByteBufferSource::ByteCount() const {
  return byte_count_;
}