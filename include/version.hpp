//
// Copyright (c) 2023-present DeepGrace (complex dot invoke at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/deepgrace/srpc
//

#ifndef SRPC_VERSION_HPP
#define SRPC_VERSION_HPP

#define SRPC_STRINGIZE(T) #T

/*
 *   SRPC_VERSION_NUMBER
 *
 *   Identifies the API version of srpc.
 *   This is a simple integer that is incremented by one every
 *   time a set of code changes is merged to the master branch.
 */

#define SRPC_VERSION_NUMBER 1
#define SRPC_VERSION_STRING "srpc/" SRPC_STRINGIZE(SRPC_VERSION_NUMBER)

#endif
