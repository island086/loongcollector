/*
 * Copyright 2022 iLogtail Authors
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

#pragma once

#include <cstdint>

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "json/json.h"

#include "constants/TagConstants.h"

namespace logtail {

struct Mount {
    std::string mSource;
    std::string mDestination;
    Mount(const std::string& source, const std::string& destination) : mSource(source), mDestination(destination) {}
    Mount() = default;
};

struct K8sInfo {
    std::string mNamespace;
    std::string mPod;
    std::string mContainerName;
    std::unordered_map<std::string, std::string> mLabels;
    bool mPausedContainer;
};

struct RawContainerInfo {
    std::string mID;
    // 标准输出
    std::string mLogPath;
    // rootfs
    std::string mUpperDir;
    // 挂载
    std::vector<Mount> mMounts;
    // 原始k8s信息
    K8sInfo mK8sInfo;
    // 环境变量信息
    std::unordered_map<std::string, std::string> mEnv;
    // 容器标签信息
    std::unordered_map<std::string, std::string> mContainerLabels;
};


struct ContainerInfo {
    // 原始容器信息
    std::shared_ptr<RawContainerInfo> mRawContainerInfo;

    std::string mID; // id of this container
    // container path for this config's path. eg, config path '/home/admin', container path
    // '/host_all/var/lib/xxxxxx/upper/home/admin' if config is wildcard, this will mapping to config->mWildcardPaths[0]
    std::string mRealBaseDir;

    std::string mLogPath;
    std::string mUpperDir;
    std::vector<Mount> mMounts; // mounts of this container
    std::vector<std::pair<std::string, std::string>> mTags; // ExternalEnvTag and ExternalK8sLabelTag.
    std::vector<std::pair<TagKey, std::string>> mMetadatas; //  ContainerNameTag
    Json::Value mJson; // this obj's json, for saving to local file
    bool mStopped = false; // whether this container is stopped


    K8sInfo mK8sInfo;
    std::unordered_map<std::string, std::string> mEnv;
    std::unordered_map<std::string, std::string> mContainerLabels;

    static bool ParseAllByJSONObj(const Json::Value& paramsAll,
                                      std::unordered_map<std::string, ContainerInfo>& containerInfoMap,
                                      std::string& errorMsg);
    static bool ParseByJSONObj(const Json::Value& params, ContainerInfo& containerInfo, std::string& errorMsg);
    void AddMetadata(const std::string& key, const std::string& value);

    bool operator==(const ContainerInfo& rhs) const {
        if (mID != rhs.mID) {
            return false;
        }
        if (mRealBaseDir != rhs.mRealBaseDir) {
            return false;
        }
        if (mLogPath != rhs.mLogPath) {
            return false;
        }
        if (mUpperDir != rhs.mUpperDir) {
            return false;
        }
        if (mMounts.size() != rhs.mMounts.size()) {
            return false;
        }
        for (size_t idx = 0; idx < mMounts.size(); ++idx) {
            const auto& lhsMount = mMounts[idx];
            const auto& rhsMount = rhs.mMounts[idx];
            if (lhsMount.mSource != rhsMount.mSource || lhsMount.mDestination != rhsMount.mDestination) {
                return false;
            }
        }
        if (mMetadatas.size() != rhs.mMetadatas.size()) {
            return false;
        }
        for (size_t idx = 0; idx < mMetadatas.size(); ++idx) {
            const auto& lhsTag = mMetadatas[idx];
            const auto& rhsTag = rhs.mMetadatas[idx];
            if (lhsTag.first != rhsTag.first || lhsTag.second != rhsTag.second) {
                return false;
            }
        }
        if (mTags.size() != rhs.mTags.size()) {
            return false;
        }
        for (size_t idx = 0; idx < mTags.size(); ++idx) {
            const auto& lhsTag = mTags[idx];
            const auto& rhsTag = rhs.mTags[idx];
            if (lhsTag.first != rhsTag.first || lhsTag.second != rhsTag.second) {
                return false;
            }
        }
        return true;
    }
    bool operator!=(const ContainerInfo& rhs) const { return !(*this == rhs); }

private:
};

} // namespace logtail
