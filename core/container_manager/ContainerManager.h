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



namespace logtail {

class ContainerManager {
public:
    void UpdateAllContainers();
    void GetAllAcceptedInfoV2(
        std::unordered_map<std::string, bool>& fullList,
        std::unordered_map<std::string, std::shared_ptr<RawContainerInfo>>& matchList,
        const std::unordered_map<std::string, std::string>& includeLabel,
        const ContainerFilters& filters);

private:
    std::unordered_map<std::string, std::shared_ptr<RawContainerInfo>> mContainerMap;
    uint32_t mLastUpdateTime = 0;

};

}