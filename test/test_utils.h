/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#include <assert.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <regex>
#include <string>
#include <thread>

using namespace std::chrono_literals;

inline void show_spinner(const std::string &prefix)
{
    static char bars[] = {'/', '-', '\\', '|'};
    static int nbars = sizeof(bars) / sizeof(char);
    static int pos = 0;

    std::cout << '\r' << prefix << bars[pos];
    std::cout.flush();
    pos = (pos + 1) % nbars;
};

inline auto now()
{
    return std::chrono::high_resolution_clock::now();
};

struct separate_thousands : std::numpunct<char>
{
    char_type do_thousands_sep() const override
    {
        return ',';
    }  // separate with commas
    string_type do_grouping() const override
    {
        return "\3";
    }  // groups of 3 digit
};

inline void test_run_timer(std::atomic<bool> &interrupt,
                           const uint32_t duration,
                           bool show_running_time = true)
{
    auto start = now();
    uint32_t time_last = 0;
    while (time_last < duration)
    {
        time_last =
            std::chrono::duration_cast<std::chrono::seconds>(now() - start)
                .count();
        if (show_running_time)
        {
            std::cout << "\r"
                      << "Test running time: " << time_last << " seconds";
        }
        std::cout.flush();
        std::this_thread::sleep_for(1000ms);
    }

    interrupt.store(true, std::memory_order_release);
}

using HighResolutionTimePoint =
    std::chrono::time_point<std::chrono::high_resolution_clock>;

inline uint64_t duration(const HighResolutionTimePoint start,
                         const HighResolutionTimePoint end)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
        .count();
}

inline uint64_t duration_micro(const HighResolutionTimePoint start,
                               const HighResolutionTimePoint end)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start)
        .count();
}

inline uint64_t qps(uint64_t count, uint64_t duration /*in milliseconds*/)
{
    if (duration == 0)
    {
        return 0;
    }
    return static_cast<uint64_t>(static_cast<double>(count) / duration * 1000);
}

inline uint64_t throughput(uint64_t size, uint64_t duration /*in milliseconds*/)
{
    if (duration == 0)
    {
        return 0;
    }
    return static_cast<uint64_t>(static_cast<double>(size) / 1024 / 1024 /
                                 duration * 1000);
}