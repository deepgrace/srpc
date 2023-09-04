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

#include <chrono>
#include <random>
#include <thread>
#include <iostream>
#include <srpc.hpp>
#include <arith.pb.h>

namespace net = boost::asio;
namespace gp = google::protobuf;

using steady_t = std::chrono::steady_clock;
using time_point_t = typename steady_t::time_point;

struct task
{
    srpc::controller controller;

    pb::request request;
    pb::response response;

    srpc::Closure* done;

    time_point_t time_begin;
    time_point_t time_end;
};

class client
{
public:
    client(snp::asio_context& ctx, const std::string& host, const std::string& port) : ctx(ctx), work(ctx.get_io_context()), host(host), port(port), dist(0, 100)
    {
        channel = new srpc::channel(ctx);
        service = new pb::service::Stub(channel, pb::service::STUB_OWNS_CHANNEL);
    }

    void invoke()
    {
        auto t = std::make_shared<task>();
        auto& controller = t->controller;

        controller.host(host);
        controller.port(port);

        controller.timeout(80);

        auto& request = t->request;
        auto op = pb::opcode(dist(generator) % 5);

        request.set_op(op);

        request.set_lhs(dist(generator));
        request.set_rhs(dist(generator));

        std::cout << "requqest: " << request.DebugString();

        t->time_begin = steady_t::now();
        t->done = gp::NewCallback(this, &client::done, t);

        service->compute(&t->controller, &t->request, &t->response, t->done);
    }

    void done(std::shared_ptr<task> t)
    {
        t->time_end = steady_t::now();
        auto& controller = t->controller;

        auto period = t->time_end - t->time_begin;
        auto number = std::chrono::duration_cast<std::chrono::microseconds>(period).count();

        if (controller.Failed())
            std::cerr << "ErrorCode: " << controller.ErrorCode() << " ErrorText: " << controller.ErrorText() << std::endl;

        std::cout << "response " << t->response.DebugString();
        std::cout << "it takes " << number << " us" << std::endl;
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

    std::default_random_engine generator;
    std::uniform_int_distribution<int> dist;
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

    constexpr size_t size = 20;
    char buff[size];

    while (fgets(buff, size, stdin))
           c.invoke();

    t.join();

    return 0;
}
