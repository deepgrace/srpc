//
// Copyright (c) 2023-present DeepGrace (complex dot invoke at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/deepgrace/srpc
//

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
