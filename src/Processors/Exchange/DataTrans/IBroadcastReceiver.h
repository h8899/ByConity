/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
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
 */

#pragma once

#include <variant>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Common/time.h>
#include <common/types.h>

namespace DB
{
using RecvDataPacket = std::variant<Chunk, BroadcastStatus>;
class IBroadcastReceiver
{
public:
    virtual void registerToSenders(UInt32 timeout_ms) = 0;
    virtual RecvDataPacket recv(UInt32 timeout_ms)
    {
        UInt64 timeout_ms_ts = time_in_milliseconds(std::chrono::system_clock::now()) + timeout_ms;
        timespec timeout_ts {.tv_sec = long(timeout_ms_ts/1000), .tv_nsec = long(timeout_ms_ts % 1000) * 1000000};
        return recv(timeout_ts);
    }
    virtual RecvDataPacket recv(timespec timeout_ts) = 0;
    virtual BroadcastStatus finish(BroadcastStatusCode status_code_, String message) = 0;
    virtual String getName() const = 0;
    virtual ~IBroadcastReceiver() = default;
};

}
