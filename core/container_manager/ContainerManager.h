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

#include <string>
#include <unordered_map>
#include <vector>
#include "file_server/ContainerInfo.h"
#include "container_manager/ContainerDiscoveryOptions.h"
#include "file_server/FileDiscoveryOptions.h"
#include "common/Thread.h"
#include "file_server/event/Event.h"
#include "container_manager/ContainerDiff.h"


namespace logtail {

class ContainerManager {
public:
    ContainerManager();
    ~ContainerManager();
    static ContainerManager* GetInstance();
    void Init();
    void Run();
    void Stop();
    void DoUpdateContainerPaths();
    bool CheckContainerUpdate();
    bool CheckConfigContainerUpdate(FileDiscoveryOptions* options, const CollectionPipelineContext* ctx);
    void UpdateAllContainers();
    void UpdateDiffContainers();

    void GetContainerStoppedEvents(std::vector<Event*>& eventVec);
    // Persist/restore container runtime state
    void SaveContainerInfo();
    void LoadContainerInfo();
    
    void GetMatchedContainersInfo(
        std::set<std::string>& fullList,
        ContainerDiff& diff,
        const std::unordered_map<std::string, std::shared_ptr<RawContainerInfo>>& matchList,
        const ContainerFilters& filters);

private:
    std::unordered_map<std::string, std::shared_ptr<RawContainerInfo>> mContainerMap;
    std::unordered_map<std::string, std::shared_ptr<ContainerDiff>> mConfigContainerDiffMap;
    std::mutex mContainerMapMutex;
    std::vector<std::string> mStoppedContainerIDs;
    std::mutex mStoppedContainerIDsMutex;
    
    uint32_t mLastUpdateTime = 0;
    ThreadPtr mThread;

    std::atomic<bool> mIsRunning{false};
    friend class ContainerManagerUnittest;
};

}