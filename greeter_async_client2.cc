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

// bool GrpcParseProto(const ::grpc::ByteBuffer& src,
//                     HelloReply* dst) {
//   struct ByteSource : public TensorResponse::Source {
//     const ::grpc::ByteBuffer* buffer;
//     GrpcByteBufferSource src;
//     bool ok;

//     ::protobuf::io::ZeroCopyInputStream* contents() {
//       ok = src.Init(*buffer);
//       return &src;
//     }
//   };
//   ByteSource bs;
//   bs.buffer = &src;
//   return dst->ParseFrom(&bs).ok() && bs.ok;
// }

class AsyncClientCallDirect {
 public:
  AsyncClientCallDirect(const std::string& user,
                        ::grpc::CompletionQueue* cq,
                        ::grpc::GenericStub* stub) {
    // encode a message directly
    HelloRequest request;
    request.set_name(user);
    auto* pl = request.mutable_payload();
    pl = reinterpret_cast<std::string*>(GenPayload(1024*1024*8));
    RequestToByteBuffer(request, &request_buf_);
    
    call_ = std::move(stub->Call(&context_, "/helloworld.Greeter/SayHello", cq, this));
    call_times = 0;
    std::cout << "create new call" << std::endl;
    {
        std::lock_guard<std::mutex> lock(mu_);
        call_cond_ = 1;
    }
    cond_.notify_one();
  }
  void OnComplete() {
      std::cout << "call time: " << call_times << std::endl;
      if (call_times == 0) {
        std::unique_lock<std::mutex> lock(mu_);
        cond_.wait(lock, [this]{return this->call_cond_ == 1;});

        call_->Write(request_buf_, this);
        call_->Read(&response_buf_, this);
        call_->Finish(&status, this);
        lock.unlock();
      } else {
        // parse response_buf_
        if (status.ok()) {
            HelloReply reply;
            GrpcParseProto(response_buf_, &reply);
            std::cout << "call end ok: " << response_buf_.Length() << std::endl;
            std::cout << "reply: " << reply.message() << std::endl;
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
    // std::unique_ptr<ClientAsyncResponseReader<HelloReply>> response_reader;
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

    // Assembles the client's payload and sends it to the server.
    void SayHello(const std::string& user) {
        // Data we are sending to the server.
        HelloRequest request;
        request.set_name(user);
        request.set_payload(reinterpret_cast<char*>(GenPayload(1024*1024*8)));

        // Call object to store rpc data
        AsyncClientCall* call = new AsyncClientCall;

        // stub_->PrepareAsyncSayHello() creates an RPC object, returning
        // an instance to store in "call" but does not actually start the RPC
        // Because we are using the asynchronous API, we need to hold on to
        // the "call" instance in order to get updates on the ongoing RPC.
        call->response_reader =
            stub_->PrepareAsyncSayHello(&call->context, request, &cq_);

        // StartCall initiates the RPC call
        call->response_reader->StartCall();

        // Request that, upon completion of the RPC, "reply" be updated with the
        // server's response; "status" with the indication of whether the operation
        // was successful. Tag the request with the memory address of the call object.
        call->response_reader->Finish(&call->reply, &call->status, (void*)call);

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
            GPR_ASSERT(ok);
            call->OnComplete();

            // if (call->status.ok())
            //     std::cout << "Greeter received: " << call->response_buf_.Length() << std::endl;
            // else
            //     std::cout << "RPC failed" << std::endl;

            // // Once we're complete, deallocate the call object.
            // delete call;
        }
    }

  private:

    // struct for keeping state and data information
    struct AsyncClientCall {
        // Container for the data we expect from the server.
        HelloReply reply;

        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // Storage for the status of the RPC upon completion.
        Status status;


        std::unique_ptr<ClientAsyncResponseReader<HelloReply>> response_reader;
    };

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
