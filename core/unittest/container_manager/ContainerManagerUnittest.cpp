// Copyright 2023 iLogtail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <memory>
#include <string>

#include "json/json.h"

#include "collection_pipeline/CollectionPipelineContext.h"
#include "common/JsonUtil.h"
#include "container_manager/ContainerManager.h"
#include "unittest/Unittest.h"

using namespace std;

namespace logtail {

class ContainerManagerUnittest : public testing::Test {
public:
    void TestGetAllAcceptedInfoV2() const;

private:
    const string pluginType = "test";
    CollectionPipelineContext ctx;
};

void ContainerManagerUnittest::TestGetAllAcceptedInfoV2() const {
    ContainerManager containerManager;
    
    std::set<std::string> fullList;
    std::unordered_map<std::string, std::shared_ptr<ContainerInfo>> matchList;
    {
        // test empty filter
        ContainerFilters filters;
        containerManager.GetAllAcceptedInfoV2(fullList, matchList, filters);
        EXPECT_EQ(fullList.size(), 0);
        EXPECT_EQ(matchList.size(), 0);
    }

    {
        // test env filter
        ContainerFilters filters;
        ContainerInfo containerInfo1;
        containerInfo1.mID = "123";
        containerInfo1.mRealBaseDir = "/var/lib/docker/containers/123";
        containerInfo1.mLogPath = "/var/lib/docker/containers/123/logs";
        containerInfo1.mEnv["test"] = "test";
        containerManager.mContainerMap["123"] = std::make_shared<ContainerInfo>(containerInfo1);

        ContainerInfo containerInfo2;
        containerInfo2.mID = "1234";
        containerInfo2.mRealBaseDir = "/var/lib/docker/containers/1234";
        containerInfo2.mLogPath = "/var/lib/docker/containers/1234/logs";
        containerInfo2.mEnv["test"] = "test2";
        containerManager.mContainerMap["1234"] = std::make_shared<ContainerInfo>(containerInfo2);

        matchList.clear();
        fullList.clear();
        filters.mEnvFilter.mIncludeFields.mFieldsMap["test"] = "test";
        containerManager.GetAllAcceptedInfoV2(fullList, matchList, filters);
        EXPECT_EQ(fullList.size(), 2);
        EXPECT_EQ(matchList.size(), 1);
        EXPECT_EQ(matchList.find("123") != matchList.end(), true);
    }   

    {
        // test k8s filter
        ContainerFilters filters;
        ContainerInfo containerInfo1;
        containerInfo1.mID = "123";
        containerInfo1.mRealBaseDir = "/var/lib/docker/containers/123";
        containerInfo1.mLogPath = "/var/lib/docker/containers/123/logs";
        containerInfo1.mK8sInfo.mPod = "pod1";
        containerInfo1.mK8sInfo.mNamespace = "namespace1";
        containerInfo1.mK8sInfo.mContainerName = "container1";
        containerManager.mContainerMap["123"] = std::make_shared<ContainerInfo>(containerInfo1);

        ContainerInfo containerInfo2;
        containerInfo2.mID = "1234";
        containerInfo2.mRealBaseDir = "/var/lib/docker/containers/1234";
        containerInfo2.mLogPath = "/var/lib/docker/containers/1234/logs";
        containerInfo2.mK8sInfo.mPod = "pod2";
        containerInfo2.mK8sInfo.mNamespace = "namespace2";
        containerInfo2.mK8sInfo.mContainerName = "container2";
        containerManager.mContainerMap["1234"] = std::make_shared<ContainerInfo>(containerInfo2);

        matchList.clear();
        fullList.clear();
        filters.mK8SFilter.mPodReg = std::make_shared<boost::regex>("pod1");
        filters.mK8SFilter.mNamespaceReg = std::make_shared<boost::regex>("namespace1");
        filters.mK8SFilter.mContainerReg = std::make_shared<boost::regex>("container1");
        containerManager.GetAllAcceptedInfoV2(fullList, matchList, filters);
        EXPECT_EQ(fullList.size(), 2);
        EXPECT_EQ(matchList.size(), 1);
        EXPECT_EQ(matchList.find("123") != matchList.end(), true);
    }
}

UNIT_TEST_CASE(ContainerManagerUnittest, TestGetAllAcceptedInfoV2)

} // namespace logtail

UNIT_TEST_MAIN
