/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>
#include <atomic>

#include <grpc++/generic/generic_stub.h>
#include <grpc++/grpc++.h>
#include <grpc++/support/byte_buffer.h>
#include <grpc++/support/slice.h>
#include <grpc/support/log.h>
#include <thread>

#include "helloworld.grpc.pb.h"

#include "grpc_util.h"
#include "proto_encode_helper.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using helloworld::HelloRequest;
using helloworld::HelloReply;
using helloworld::Greeter;

void* GenPayload(const size_t size) {
    std::string* ret = new std::string();
    ret->reserve(size);
    std::cout << "alloc " << ret->capacity() << std::endl;
    for (int i=0; i<size; ++i) ret->append("x");
    return ret;
}

void RequestToByteBuffer(const HelloRequest& proto,
                         ::grpc::ByteBuffer* result) {
  ::grpc::Slice slice(proto.ByteSizeLong());
  proto.SerializeWithCachedSizesToArray(
      const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(slice.begin())));
  ::grpc::ByteBuffer tmp(&slice, 1);
  result->Swap(&tmp);
}

// sample to parse to raw protobuf message
//
// bool GrpcParseProto(const grpc::ByteBuffer& src, protobuf::Message* dst) {
//   GrpcByteBufferSource stream;
//   if (!stream.Init(src)) return false;
//   return dst->ParseFromZeroCopyStream(&stream);
// }

bool GrpcParseProto(const grpc::ByteBuffer& src, HelloReply* dst) {
  GrpcByteBufferSource stream;
  if (!stream.Init(src)) return false;
  return dst->ParseFromZeroCopyStream(&stream);
}

void GenRequestByteBuffer(const std::string& user,
                          char* payload,
                          int size,
                          ::grpc::ByteBuffer *result) {
    HelloRequest request;
    std::string header;
    request.AppendToString(&header);

    void* buf = malloc(128);
    ProtoEncodeHelper e((char*)buf, 128);
    e.WriteRawBytes(header);  // protobuf header
    // e.WriteVarlengthBeginning(1, user.size() + size);
    e.WriteString(1, user);
    e.WriteVarlengthBeginning(2, size);

    ::grpc::Slice slices[2];  // name and payload
    slices[0] = ::grpc::Slice(e.size());
    memcpy(const_cast<uint8_t*>(slices[0].begin()), e.data(), e.size());
    slices[1] = ::grpc::Slice(
        grpc_slice_new_with_user_data(
              const_cast<void*>(static_cast<const void*>(payload)),
              size,
              [](void* backing) {
                // static_cast<TensorBuffer*>(backing)->Unref();
              },
              const_cast<char*>(payload)),
        ::grpc::Slice::STEAL_REF);
    ::grpc::ByteBuffer tmp(&slices[0], 2);
    result->Swap(&tmp);
    std::cout << "piled buffer " << result->Length();
}


class AsyncClientCallDirect {
 public:
  AsyncClientCallDirect(const std::string& user,
                        ::grpc::CompletionQueue* cq,
                        ::grpc::GenericStub* stub) {
    // encode a message directly
    const int size = 3 * 1024 * 1024;
    char* payload_alloc = (char*)GenPayload(size);

    double ts = GetTimestamp();
    // HelloRequest request;
    // request.set_name(user);
    // // This will copy
    // request.set_allocated_payload(reinterpret_cast<std::string*>(payload_alloc));
    // auto* pl = request.mutable_payload();
    // this have no affect
    // pl = reinterpret_cast<std::string*>(payload_alloc);
    // RequestToByteBuffer(request, &request_buf_);
    ::grpc::ByteBuffer request;
    GenRequestByteBuffer(user, payload_alloc, size, &request_buf_);
    printf("time is %.2f ms\n", GetTimestamp() - ts);

    call_ = std::move(stub->Call(&context_, "/helloworld.Greeter/SayHello", cq, this));
    call_times = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        call_cond_ = 1;
    }
    cond_.notify_one();
  }
  void OnComplete(bool ok) {
      if (call_times == 0) {
        std::unique_lock<std::mutex> lock(mu_);
        cond_.wait(lock, [this]{return this->call_cond_ == 1;});
        if (ok) {
            call_->Write(request_buf_, this);
            call_->Read(&response_buf_, this);
        }
        call_->Finish(&status, this);
        lock.unlock();
      } else if (call_times == 3) {
        // parse response_buf_
        if (status.ok()) {
            HelloReply reply;
            GrpcParseProto(response_buf_, &reply);
        } else {
            std::cout << status.error_message() << std::endl;
        }
        delete this;
      }
      call_times++;
  }
  Status status;
  HelloReply reply;
  ::grpc::ByteBuffer response_buf_;

 private:
    std::atomic<int> call_times;
    ::grpc::ByteBuffer request_buf_;
    ClientContext context_;
    std::unique_ptr<::grpc::GenericClientAsyncReaderWriter> call_;

    std::mutex mu_;
    std::condition_variable cond_;
    int call_cond_ = 0;
};


class GreeterClient {
  public:
    explicit GreeterClient(std::shared_ptr<Channel> channel)
            : stub_(Greeter::NewStub(channel)), g_stub_(channel) {}

    void SayHelloDirect(const std::string& user) {
        AsyncClientCallDirect* call = new AsyncClientCallDirect(user, &cq_, &g_stub_);
    }

    // Loop while listening for completed responses.
    // Prints out the response from the server.
    void AsyncCompleteRpc() {
        void* got_tag;
        bool ok = false;

        // Block until the next result is available in the completion queue "cq".
        while (cq_.Next(&got_tag, &ok)) {
            // The tag in this example is the memory location of the call object
            AsyncClientCallDirect* call = static_cast<AsyncClientCallDirect*>(got_tag);
            
            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            call->OnComplete(ok);
        }
    }

  private:
    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    std::unique_ptr<Greeter::Stub> stub_;
    ::grpc::GenericStub g_stub_;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq_;
};

int main(int argc, char** argv) {


    // Instantiate the client. It requires a channel, out of which the actual RPCs
    // are created. This channel models a connection to an endpoint (in this case,
    // localhost at port 50051). We indicate that the channel isn't authenticated
    // (use of InsecureChannelCredentials()).
    GreeterClient greeter(grpc::CreateChannel(
            "localhost:50051", grpc::InsecureChannelCredentials()));

    // Spawn reader thread that loops indefinitely
    std::thread thread_ = std::thread(&GreeterClient::AsyncCompleteRpc, &greeter);

    for (int i = 0; i < 100; i++) {
        std::string user("world " + std::to_string(i));
        greeter.SayHelloDirect(user);  // The actual RPC call!
    }

    std::cout << "Press control-c to quit" << std::endl << std::endl;
    thread_.join();  //blocks forever

    return 0;
}
