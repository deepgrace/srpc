# srpc [![LICENSE](https://img.shields.io/github/license/deepgrace/srpc.svg)](https://github.com/deepgrace/srpc/blob/main/LICENSE_1_0.txt) [![Language](https://img.shields.io/badge/language-C%2B%2B20-blue.svg)](https://en.cppreference.com/w/cpp/compiler_support) [![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20MacOS%20%7C%20Windows-lightgrey.svg)](https://github.com/deepgrace/srpc)

> **Sender based Asynchronous Rpc**

## Overview
An implementation of the *ping / pong* service.  

**protobuf message definition**:
```protobuf
syntax = "proto3";

package pb;

message request
{
    optional bytes command = 1;
}

message response
{
    optional bytes results = 1;
}

service service
{
    rpc execute(request) returns (response);
}

option cc_generic_services = true;
```

**client side implementation**:
```cpp
#define BOOST_ASIO_HAS_IO_URING
#define BOOST_ASIO_DISABLE_EPOLL

#include <thread>
#include <iostream>
#include <srpc.hpp>
#include <ping.pb.h>

namespace net = boost::asio;
namespace gp = google::protobuf;

struct task
{
    srpc::controller controller;

    pb::request request;
    pb::response response;

    srpc::Closure* done;
};

class client
{
public:
    client(snp::asio_context& ctx, const std::string& host, const std::string& port) : ctx(ctx), work(ctx.get_io_context()), host(host), port(port)
    {
        channel = new srpc::channel(ctx);
        service = new pb::service::Stub(channel, pb::service::STUB_OWNS_CHANNEL);
    }

    void ping()
    {
        auto t = std::make_shared<task>();
        auto& controller = t->controller;

        controller.host(host);
        controller.port(port);

        controller.timeout(80);

        t->request.set_command("ping");
        t->done = gp::NewCallback(this, &client::done, t);

        service->execute(&t->controller, &t->request, &t->response, t->done);
    }

    void done(std::shared_ptr<task> t)
    {
        auto& controller = t->controller;

        if (controller.Failed())
            std::cerr << "ErrorCode: " << controller.ErrorCode() << " ErrorText: " << controller.ErrorText() << std::endl;

        std::cout << t->response.DebugString();
    }

    ~client()
    {
        delete service;
    }

private:
    snp::asio_context& ctx;
    net::io_context::work work;

    srpc::channel* channel;
    pb::service* service;

    std::string host;
    std::string port;
};

int main(int argc, char* argv[])
{
    if (argc != 3)
    {
        std::cout << "Usage: " << argv[0] << " <host> <port>" << std::endl;

        return 1;
    }

    std::string host(argv[1]);
    std::string port(argv[2]);

    snp::asio_context ctx;
    client c(ctx, host, port);

    std::thread t([&]{ ctx.run(); });

    constexpr size_t size = 100;
    char buff[size];

    while (fgets(buff, size, stdin))
           c.ping();

    t.join();

    return 0;
}
```

**server side implementation**:
```cpp
#define BOOST_ASIO_HAS_IO_URING
#define BOOST_ASIO_DISABLE_EPOLL

#include <iostream>
#include <srpc.hpp>
#include <ping.pb.h>

namespace net = boost::asio;
namespace gp = google::protobuf;

void done()
{
    std::cout << "got called" << std::endl;
}

class service : public pb::service
{
public:
    service()
    {
    }

    void execute(gp::RpcController* controller, const pb::request* request, pb::response* response, gp::Closure* done)
    {
        std::cout << request->DebugString();

        if (request->command() != "ping")        
            controller->SetFailed("unknown command");
        else
            response->set_results("pong");

        done->Run();
    }

    ~service()
    {
    }
};

int main(int argc, char* argv[])
{
    if (argc != 3)
    {
        std::cout << "Usage: " << argv[0] << " <host> <port>" << std::endl;

        return 1;
    }

    std::string host(argv[1]);
    std::string port(argv[2]);

    snp::asio_context ctx;

    service s;
    srpc::server server(ctx, host, port);
    
    server.register_service(&s, gp::NewPermanentCallback(&done));
    server.run();

    ctx.run();

    return 0;
}
```

## Introduction
srpc is a **Remote Procedure Call** (**RPC**) library, which is header-only, extensible and modern C++ oriented.  
It's built on top off the **snp** and protobuf, it's inspired by the C++ *Sender / Receiver* async programming model.  
srpc enables you to do network programming with tcp protocol in a straightforward, asynchronous and OOP manner.  

srpc provides the following features:
- **timeout**     The upper limit of the total time for the call to time out between a RPC request and response
- **callback**    The callable to be invoked immediately after a RPC request or response has been accepted and processed  
- **controller**  A way to manipulate settings specific to the RPC implementation and to find out about RPC-level errors

## Prerequsites
[snp](https://github.com/deepgrace/snp)  
[protobuf](https://github.com/protocolbuffers/protobuf)  

By default, the example is using the io_uring backend, it's disabled by default in Boost.Asio.  
This backend may be used for all I/O objects, including sockets, timers, and posix descriptors.  
To disable the io_uring backend, just comment out the following lines in code under the example directory:
```cpp
#define BOOST_ASIO_HAS_IO_URING
#define BOOST_ASIO_DISABLE_EPOLL
```

## Compiler requirements
The library relies on a C++20 compiler and standard library

More specifically, srpc requires a compiler/standard library supporting the following C++20 features (non-exhaustively):
- concepts
- lambda templates
- All the C++20 type traits from the <type_traits> header

## Building
srpc is header-only. To use it just add the necessary `#include` line to your source files, like this:
```cpp
#include <srpc.hpp>
```

`git clone https://github.com/deepgrace/snp.git` and place it with srpc under the same directory.  
To build the example with cmake, `cd` to the root of the project and setup the build directory:
```bash
mkdir build
cd build
cmake ..
```

Make and install the executables:
```
make -j4
make install
```

The executables are now located at the `bin` directory of the root of the project.  
The example can also be built with the script `build.sh`, just run it, the executables will be put at the `/tmp` directory.

## Full example
Please see [example](example).

## License
srpc is licensed as [Boost Software License 1.0](LICENSE_1_0.txt).
