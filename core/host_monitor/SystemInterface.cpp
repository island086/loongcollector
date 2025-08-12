/*
 * Copyright 2025 iLogtail Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "host_monitor/SystemInterface.h"

#include <boost/asio.hpp>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/address_v6.hpp>
#include <chrono>
#include <mutex>
#include <tuple>
#include <utility>

#include "boost/type_index.hpp"

#include "common/Flags.h"
#include "logger/Logger.h"
#ifdef __linux__
#include "host_monitor/LinuxSystemInterface.h"
#endif
#ifdef APSARA_UNIT_TEST_MAIN
#include "unittest/host_monitor/MockSystemInterface.h"
#endif

DEFINE_FLAG_INT32(system_interface_cache_queue_size, "system interface default cache size", 15);
DEFINE_FLAG_INT32(system_interface_cache_entry_expire_seconds, "cache entry expire time in minutes", 10);
DEFINE_FLAG_INT32(system_interface_cache_cleanup_interval_seconds, "cache cleanup interval in minutes", 5);
DEFINE_FLAG_INT32(system_interface_cache_max_cleanup_batch_size, "max entries to cleanup in one batch", 50);

namespace logtail {

SystemInterface* SystemInterface::GetInstance() {
#ifdef APSARA_UNIT_TEST_MAIN
    return MockSystemInterface::GetInstance();
#endif
#ifdef __linux__
    return LinuxSystemInterface::GetInstance();
#else
    LOG_ERROR(sLogger, "SystemInterface is not implemented for this platform");
    return nullptr;
#endif
}

bool SystemInterface::GetSystemInformation(SystemInformation& systemInfo) {
    // SystemInformation is static and will not be changed. So cache will never be expired.
    if (mSystemInformationCache.collectTime.time_since_epoch().count() > 0) {
        systemInfo = mSystemInformationCache;
        return true;
    }
    if (GetSystemInformationOnce(mSystemInformationCache)) {
        systemInfo = mSystemInformationCache;
        return true;
    }
    return false;
}

bool SystemInterface::GetCPUInformation(std::chrono::steady_clock::time_point now, CPUInformation& cpuInfo) {
    const std::string errorType = "cpu";
    return MemoizedCall(
        mCPUInformationCache,
        now,
        [this](BaseInformation& info) { return this->GetCPUInformationOnce(static_cast<CPUInformation&>(info)); },
        cpuInfo,
        errorType);
}

bool SystemInterface::GetProcessListInformation(std::chrono::steady_clock::time_point now,
                                                ProcessListInformation& processListInfo) {
    const std::string errorType = "process list";
    return MemoizedCall(
        mProcessListInformationCache,
        now,
        [this](BaseInformation& info) {
            return this->GetProcessListInformationOnce(static_cast<ProcessListInformation&>(info));
        },
        processListInfo,
        errorType);
}

bool SystemInterface::GetProcessInformation(std::chrono::steady_clock::time_point now,
                                            pid_t pid,
                                            ProcessInformation& processInfo) {
    const std::string errorType = "process";
    return MemoizedCall(
        mProcessInformationCache,
        now,
        [this](BaseInformation& info, pid_t pid) {
            return this->GetProcessInformationOnce(pid, static_cast<ProcessInformation&>(info));
        },
        processInfo,
        errorType,
        pid);
}

bool SystemInterface::GetSystemLoadInformation(std::chrono::steady_clock::time_point now,
                                               SystemLoadInformation& systemLoadInfo) {
    const std::string errorType = "system load";
    return MemoizedCall(
        mSystemLoadInformationCache,
        now,
        [this](BaseInformation& info) {
            return this->GetSystemLoadInformationOnce(static_cast<SystemLoadInformation&>(info));
        },
        systemLoadInfo,
        errorType);
}

bool SystemInterface::GetCPUCoreNumInformation(CpuCoreNumInformation& cpuCoreNumInfo) {
    if (mCPUCoreNumInformationCache.collectTime.time_since_epoch().count() > 0) {
        cpuCoreNumInfo = mCPUCoreNumInformationCache;
        return true;
    }
    if (GetCPUCoreNumInformationOnce(mCPUCoreNumInformationCache)) {
        cpuCoreNumInfo = mCPUCoreNumInformationCache;
        return true;
    }
    return false;
}

bool SystemInterface::GetHostMemInformationStat(std::chrono::steady_clock::time_point now, MemoryInformation& meminfo) {
    const std::string errorType = "mem";
    return MemoizedCall(
        mMemInformationCache,
        now,
        [this](BaseInformation& info) {
            return this->GetHostMemInformationStatOnce(static_cast<MemoryInformation&>(info));
        },
        meminfo,
        errorType);
}

bool SystemInterface::GetFileSystemListInformation(FileSystemListInformation& fileSystemListInfo) {
    const std::string errorType = "filesystem list";
    return MemoizedCall(
        mFileSystemListInformationCache,
        [this](BaseInformation& info) {
            return this->GetFileSystemListInformationOnce(static_cast<FileSystemListInformation&>(info));
        },
        fileSystemListInfo,
        errorType);
}

bool SystemInterface::GetSystemUptimeInformation(SystemUptimeInformation& systemUptimeInfo) {
    const std::string errorType = "system uptime";
    return MemoizedCall(
        mSystemUptimeInformationCache,
        [this](BaseInformation& info) {
            return this->GetSystemUptimeInformationOnce(static_cast<SystemUptimeInformation&>(info));
        },
        systemUptimeInfo,
        errorType);
}

bool SystemInterface::GetDiskSerialIdInformation(std::string diskName, SerialIdInformation& serialIdInfo) {
    const std::string errorType = "SerialId";
    return MemoizedCall(
        mSerialIdInformationCache,
        [this](BaseInformation& info, std::string diskName) {
            return this->GetDiskSerialIdInformationOnce(diskName, static_cast<SerialIdInformation&>(info));
        },
        serialIdInfo,
        errorType,
        diskName);
}

bool SystemInterface::GetDiskStateInformation(DiskStateInformation& diskStateInfo) {
    const std::string errorType = "disk state";
    return MemoizedCall(
        mDiskStateInformationCache,
        [this](BaseInformation& info) {
            return this->GetDiskStateInformationOnce(static_cast<DiskStateInformation&>(info));
        },
        diskStateInfo,
        errorType);
}
bool SystemInterface::GetProcessCmdlineString(pid_t pid, ProcessCmdlineString& processCmdlineString) {
    const std::string errorType = "processCmdline";
    return MemoizedCall(
        mProcessCmdlineCache,
        [this](BaseInformation& info, pid_t pid) {
            return this->GetProcessCmdlineStringOnce(pid, static_cast<ProcessCmdlineString&>(info));
        },
        processCmdlineString,
        errorType,
        pid);
}

bool SystemInterface::GetPorcessStatm(pid_t pid, ProcessMemoryInformation& processMemory) {
    const std::string errorType = "processStatm";
    return MemoizedCall(
        mProcessStatmCache,
        [this](BaseInformation& info, pid_t pid) {
            return this->GetProcessStatmOnce(pid, static_cast<ProcessMemoryInformation&>(info));
        },
        processMemory,
        errorType,
        pid);
}

bool SystemInterface::GetProcessCredNameObj(pid_t pid, ProcessCredName& credName) {
    const std::string errorType = "processStatus";
    return MemoizedCall(
        mProcessStatusCache,
        [this](BaseInformation& info, pid_t pid) {
            return this->GetProcessCredNameOnce(pid, static_cast<ProcessCredName&>(info));
        },
        credName,
        errorType,
        pid);
}

bool SystemInterface::GetExecutablePathCache(pid_t pid, ProcessExecutePath& executePath) {
    const std::string errorType = "executablePath";
    return MemoizedCall(
        mExecutePathCache,
        [this](BaseInformation& info, pid_t pid) {
            return this->GetExecutablePathOnce(pid, static_cast<ProcessExecutePath&>(info));
        },
        executePath,
        errorType,
        pid);
}

bool SystemInterface::GetProcessOpenFiles(pid_t pid, ProcessFd& processFd) {
    const std::string errorType = "processOpenFiles";
    return MemoizedCall(
        mProcessFdCache,
        [this](BaseInformation& info, pid_t pid) {
            return this->GetProcessOpenFilesOnce(pid, static_cast<ProcessFd&>(info));
        },
        processFd,
        errorType,
        pid);
}
bool SystemInterface::GetTCPStatInformation(std::chrono::steady_clock::time_point now,
                                            TCPStatInformation& tcpStatInfo) {
    const std::string errorType = "TCP stat";
    return MemoizedCall(
        mTCPStatInformationCache,
        now,
        [this](BaseInformation& info) {
            return this->GetTCPStatInformationOnce(static_cast<TCPStatInformation&>(info));
        },
        tcpStatInfo,
        errorType);
}

bool SystemInterface::GetNetInterfaceInformation(std::chrono::steady_clock::time_point now,
                                                 NetInterfaceInformation& netInterfaceInfo) {
    const std::string errorType = "Net interface";
    return MemoizedCall(
        mNetInterfaceInformationCache,
        now,
        [this](BaseInformation& info) {
            return this->GetNetInterfaceInformationOnce(static_cast<NetInterfaceInformation&>(info));
        },
        netInterfaceInfo,
        errorType);
}

template <typename F, typename InfoT, typename... Args>
bool SystemInterface::MemoizedCall(SystemInformationCache<InfoT, Args...>& cache,
                                   std::chrono::steady_clock::time_point now,
                                   F&& func,
                                   InfoT& info,
                                   const std::string& errorType,
                                   Args... args) {
    if (cache.Get(now, info, args...)) {
        return true;
    }
    bool status = std::forward<F>(func)(info, args...);
    if (status) {
        cache.Set(info, args...);
    } else {
        LOG_ERROR(sLogger, ("failed to get system information", errorType));
    }
    return status;
}

template <typename InfoT, typename... Args>
bool SystemInterface::SystemInformationCache<InfoT, Args...>::Get(std::chrono::steady_clock::time_point now,
                                                                  InfoT& info,
                                                                  Args... args) {
    std::unique_lock<std::mutex> lock(mMutex);
    auto it = mCache.find(std::make_tuple(args...));
    if (it != mCache.end()) {
        it->second.lastAccessTime = std::chrono::steady_clock::now();

        auto& data = it->second.data;
        if (data.empty()) {
            return false;
        }

        // Use binary search to find the first entry with collectTime >= now
        auto lower = std::lower_bound(
            data.begin(), data.end(), now, [](const InfoT& entry, const std::chrono::steady_clock::time_point& target) {
                return entry.collectTime < target;
            });

        if (lower != data.end()) {
            // Found entry with collectTime >= now, this is the closest entry after now
            info = *lower;
            return true;
        } else {
            // All entries have collectTime < now, no suitable entry found
            return false;
        }
    } else {
        mCache[std::make_tuple(args...)] = CacheEntry();
        return false;
    }
}

template <typename InfoT, typename... Args>
bool SystemInterface::SystemInformationCache<InfoT, Args...>::Set(InfoT& info, Args... args) {
    {
        std::lock_guard<std::mutex> lock(mMutex);
        auto& cacheEntry = mCache[std::make_tuple(args...)];
        auto& deque = cacheEntry.data;

        // Find correct position to maintain time ordering (newest at back)
        auto insertPos = std::upper_bound(deque.begin(), deque.end(), info, [](const InfoT& a, const InfoT& b) {
            return a.collectTime < b.collectTime;
        });
        if (insertPos != deque.end() && insertPos->collectTime == info.collectTime) {
            // conflict, use old value
            LOG_DEBUG(sLogger, ("system information cache conflict", "use old value instead of new value"));
            info = *insertPos;
        } else {
            deque.insert(insertPos, info);
        }

        // Remove oldest entries if size exceeds limit
        if (deque.size() > mCacheSize) {
            deque.pop_front();
        }

        // Update access time
        cacheEntry.lastAccessTime = std::chrono::steady_clock::now();
    }

    // Check if cleanup is needed (don't hold lock during cleanup)
    if (ShouldPerformCleanup()) {
        PerformGarbageCollection();
    }

    return true;
}

template <typename InfoT>
bool SystemInterface::SystemInformationCache<InfoT>::Get(std::chrono::steady_clock::time_point now, InfoT& info) {
    std::unique_lock<std::mutex> lock(mMutex);

    if (mCache.empty()) {
        return false;
    }

    // Use binary search to find the first entry with collectTime >= now
    auto lower = std::lower_bound(
        mCache.begin(), mCache.end(), now, [](const InfoT& entry, const std::chrono::steady_clock::time_point& target) {
            return entry.collectTime < target;
        });

    if (lower != mCache.end()) {
        // Found entry with collectTime >= now, this is the closest entry after now
        info = *lower;
        return true;
    } else {
        // All entries have collectTime < now, no suitable entry found
        return false;
    }
}

template <typename InfoT>
bool SystemInterface::SystemInformationCache<InfoT>::Set(InfoT& info) {
    std::lock_guard<std::mutex> lock(mMutex);

    // Find correct position to maintain time ordering (newest at back)
    auto insertPos = std::upper_bound(mCache.begin(), mCache.end(), info, [](const InfoT& a, const InfoT& b) {
        return a.collectTime < b.collectTime;
    });
    if (insertPos != mCache.end() && insertPos->collectTime == info.collectTime) {
        // conflict, use old value
        info = *insertPos;
    } else {
        mCache.insert(insertPos, info);
    }

    // Remove oldest entries if size exceeds limit
    if (mCache.size() > mCacheSize) {
        mCache.pop_front();
    }
    return true;
}

std::string MacString(const unsigned char* mac) {
    std::string str;
    if (mac != nullptr && sizeof(mac) >= 6) {
        str = fmt::format("{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}", mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
        // str = fmt::sprintf("%02X:%02X:%02X:%02X:%02X:%02X", mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
    }
    return str;
}

std::string IPv4String(uint32_t address) {
    using namespace boost::asio::ip;
    // address = boost::endian::big_to_native(address);
    address_v4 v4(*(address_v4::bytes_type*)(&address));
    return v4.to_string();
}

std::string IPv6String(const uint32_t address[4]) {
    using namespace boost::asio::ip;
    // address = boost::endian::big_to_native(address);
    address_v6 v6(*(address_v6::bytes_type*)(address));
    return v6.to_string();
}

NetAddress::NetAddress() {
    memset(this, 0, sizeof(*this));
}

static const auto& mapNetAddressStringer = *new std::map<int, std::function<std::string(const NetAddress*)>>{
    {NetAddress::SI_AF_UNSPEC, [](const NetAddress* me) { return std::string{}; }},
    {NetAddress::SI_AF_INET, [](const NetAddress* me) { return IPv4String(me->addr.in); }},
    {NetAddress::SI_AF_INET6, [](const NetAddress* me) { return IPv6String(me->addr.in6); }},
    {NetAddress::SI_AF_LINK, [](const NetAddress* me) { return MacString(me->addr.mac); }},
};

std::string NetAddress::str() const {
    std::string name;
    auto it = mapNetAddressStringer.find(this->family);
    if (it != mapNetAddressStringer.end()) {
        name = it->second(this);
    }
    return name;
}

// GC implementation for SystemInformationCache with arguments
template <typename InfoT, typename... Args>
void SystemInterface::SystemInformationCache<InfoT, Args...>::PerformGarbageCollection() {
    if (ClearExpiredEntries(mExpireThreshold)) {
        // only update last cleanup time if all expired entries are cleared
        mLastCleanupTime = std::chrono::steady_clock::now();
    }
}

template <typename InfoT, typename... Args>
bool SystemInterface::SystemInformationCache<InfoT, Args...>::ClearExpiredEntries(
    std::chrono::steady_clock::duration maxAge) {
    std::lock_guard<std::mutex> lock(mMutex);
    auto now = std::chrono::steady_clock::now();
    auto maxCleanupCount = mMaxCleanupCount;
    // if maxCleanupCount is set to 0 or negative, scan all entries
    if (maxCleanupCount <= 0) {
        maxCleanupCount = mCacheSize;
    }
    int32_t cleanedCount = 0;

    for (auto it = mCache.begin(); it != mCache.end() && cleanedCount < maxCleanupCount;) {
        if (now - it->second.lastAccessTime > maxAge) {
            it = mCache.erase(it);
            ++cleanedCount;
        } else {
            ++it;
        }
    }
    return cleanedCount < maxCleanupCount;
}

template <typename InfoT, typename... Args>
bool SystemInterface::SystemInformationCache<InfoT, Args...>::ShouldPerformCleanup() const {
    auto now = std::chrono::steady_clock::now();
    return (now - mLastCleanupTime) >= mCleanupInterval;
}

template <typename InfoT, typename... Args>
size_t SystemInterface::SystemInformationCache<InfoT, Args...>::GetCacheSize() const {
    std::lock_guard<std::mutex> lock(mMutex);
    return mCache.size();
}

} // namespace logtail
