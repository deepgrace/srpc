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
