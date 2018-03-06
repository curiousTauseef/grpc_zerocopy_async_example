#include <sys/time.h>
#include <grpc++/grpc++.h>
#include "grpc++/impl/codegen/proto_utils.h"
#include "grpc++/support/byte_buffer.h"

inline double GetTimestamp() {
    struct timeval t0;
    gettimeofday(&t0, 0);
    return double(t0.tv_sec * 1000.0 + t0.tv_usec / 1000.0);
}

// A ZeroCopyInputStream that reads from a grpc::ByteBuffer.
class GrpcByteBufferSource : public ::grpc::protobuf::io::ZeroCopyInputStream {
 public:
  GrpcByteBufferSource();
  bool Init(const ::grpc::ByteBuffer& src);  // Can be called multiple times.
  bool Next(const void** data, int* size) override;
  void BackUp(int count) override;
  bool Skip(int count) override;
  ::grpc::protobuf::int64 ByteCount() const override;

 private:
  std::vector<::grpc::Slice> slices_;
  int cur_;          // Current slice index.
  int left_;         // Number of bytes in slices_[cur_] left to yield.
  const char* ptr_;  // Address of next byte in slices_[cur_] to yield.
  ::grpc::protobuf::int64 byte_count_;
};