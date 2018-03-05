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

#include <grpc++/generic/generic_stub.h>
#include <grpc++/grpc++.h>
#include <grpc++/support/byte_buffer.h>
#include <grpc++/support/slice.h>
#include <grpc/support/log.h>
#include <thread>

#include "helloworld.grpc.pb.h"

#include "grpc_util.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using helloworld::HelloRequest;
using helloworld::HelloReply;
using helloworld::Greeter;

void* GenPayload(const size_t size) {
    void* data = malloc(size);
    return data;
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

inline int64_t GetTimestamp() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch())
    .count();
}

class AsyncClientCallDirect {
 public:
  AsyncClientCallDirect(const std::string& user,
                        ::grpc::CompletionQueue* cq,
                        ::grpc::GenericStub* stub) {
    // encode a message directly
    void* payload_alloc = GenPayload(1024*1024*8);
    start_time_ = GetTimestamp();
    HelloRequest request;
    request.set_name(user);
    auto* pl = request.mutable_payload();
    pl = reinterpret_cast<std::string*>(payload_alloc);
    RequestToByteBuffer(request, &request_buf_);
    
    call_ = std::move(stub->Call(&context_, "/helloworld.Greeter/SayHello", cq, this));
    call_times = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        call_cond_ = 1;
    }
    cond_.notify_one();
  }
  void OnComplete(bool ok) {
      std::cout << "call times: " << call_times << std::endl;
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
            std::cout << "call end ok: " << response_buf_.Length() << std::endl;
            std::cout << "reply: " << reply.message() << " timespent: "
                      << GetTimestamp() - start_time_
                      << " start time " << start_time_ << std::endl;
        } else {
            std::cout << "call end error";
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
    int64_t start_time_;

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

    for (int i = 0; i < 10; i++) {
        std::string user("world " + std::to_string(i));
        greeter.SayHelloDirect(user);  // The actual RPC call!
    }

    std::cout << "Press control-c to quit" << std::endl << std::endl;
    thread_.join();  //blocks forever

    return 0;
}
