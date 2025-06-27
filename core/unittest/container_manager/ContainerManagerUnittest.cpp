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
#include "unittest/pipeline/LogtailPluginMock.h"

using namespace std;

namespace logtail {

class ContainerManagerUnittest : public testing::Test {
public:
    void TestGetMatchedContainersInfo() const;
    void TestUpdateAllContainers() const;
    void TestUpdateDiffContainers() const;

private:
    const string pluginType = "test";
    CollectionPipelineContext ctx;
};

void ContainerManagerUnittest::TestGetMatchedContainersInfo() const {
    ContainerManager containerManager;
    
    std::set<std::string> fullList;
    std::unordered_map<std::string, std::shared_ptr<RawContainerInfo>> matchList;
    {
        // test empty filter
        ContainerFilters filters;
        containerManager.GetMatchedContainersInfo(fullList, matchList, filters);
        EXPECT_EQ(fullList.size(), 0);
        EXPECT_EQ(matchList.size(), 0);
    }

    {
        // test env filter
        ContainerFilters filters;
        RawContainerInfo containerInfo1;
        containerInfo1.mID = "123";
        containerInfo1.mLogPath = "/var/lib/docker/containers/123/logs";
        containerInfo1.mEnv["test"] = "test";
        containerManager.mContainerMap["123"] = std::make_shared<RawContainerInfo>(containerInfo1);

        RawContainerInfo containerInfo2;
        containerInfo2.mID = "1234";
        containerInfo2.mLogPath = "/var/lib/docker/containers/1234/logs";
        containerInfo2.mEnv["test"] = "test2";
        containerManager.mContainerMap["1234"] = std::make_shared<RawContainerInfo>(containerInfo2);

        matchList.clear();
        fullList.clear();
        filters.mEnvFilter.mIncludeFields.mFieldsMap["test"] = "test";
        containerManager.GetMatchedContainersInfo(fullList, matchList, filters);
        EXPECT_EQ(fullList.size(), 2);
        EXPECT_EQ(matchList.size(), 1);
        EXPECT_EQ(matchList.find("123") != matchList.end(), true);
    }   

    {
        // test k8s filter
        ContainerFilters filters;
        RawContainerInfo containerInfo1;
        containerInfo1.mID = "123";
        containerInfo1.mLogPath = "/var/lib/docker/containers/123/logs";
        containerInfo1.mK8sInfo.mPod = "pod1";
        containerInfo1.mK8sInfo.mNamespace = "namespace1";
        containerInfo1.mK8sInfo.mContainerName = "container1";
        containerManager.mContainerMap["123"] = std::make_shared<RawContainerInfo>(containerInfo1);

        RawContainerInfo containerInfo2;
        containerInfo2.mID = "1234";
        containerInfo2.mLogPath = "/var/lib/docker/containers/1234/logs";
        containerInfo2.mK8sInfo.mPod = "pod2";
        containerInfo2.mK8sInfo.mNamespace = "namespace2";
        containerInfo2.mK8sInfo.mContainerName = "container2";
        containerManager.mContainerMap["1234"] = std::make_shared<RawContainerInfo>(containerInfo2);

        matchList.clear();
        fullList.clear();
        filters.mK8SFilter.mPodReg = std::make_shared<boost::regex>("pod1");
        filters.mK8SFilter.mNamespaceReg = std::make_shared<boost::regex>("namespace1");
        filters.mK8SFilter.mContainerReg = std::make_shared<boost::regex>("container1");
        containerManager.GetMatchedContainersInfo(fullList, matchList, filters);
        EXPECT_EQ(fullList.size(), 2);
        EXPECT_EQ(matchList.size(), 1);
        EXPECT_EQ(matchList.find("123") != matchList.end(), true);
    }
}

void ContainerManagerUnittest::TestUpdateAllContainers() const {
    {
        // test empty containers meta
        LogtailPluginMock::GetInstance()->SetUpContainersMeta("");
        ContainerManager containerManager;
        containerManager.UpdateAllContainers();
        EXPECT_EQ(containerManager.mContainerMap.size(), 0);
    }
    {
        // test empty containers meta
        LogtailPluginMock::GetInstance()->SetUpContainersMeta(R"({
	"AllCmd": [{
		"ID": "9c7da0bc25f57de99283456960072b7f5ebc069599c6e5efec567c3e6e70ca93",
		"LogPath": "/var/lib/docker/containers/9c7da0bc25f57de99283456960072b7f5ebc069599c6e5efec567c3e6e70ca93/9c7da0bc25f57de99283456960072b7f5ebc069599c6e5efec567c3e6e70ca93-json.log",
		"MetaDatas": ["_namespace_", "kube-system", "_pod_uid_", "4991ae55-8a3c-4228-9668-4d4feb748ad1", "_image_name_", "aliyun-observability-release-registry.cn-shanghai.cr.aliyuncs.com/loongcollector-dev/logtail:v3.1.0.0-f57a0e2-aliyun-0612", "_container_name_", "loongcollector", "_pod_name_", "loongcollector-ds-4glk5"],
		"Mounts": [{
			"Destination": "/logtail_host",
			"Source": "/"
		}, {
			"Destination": "/sys",
			"Source": "/sys"
		}, {
			"Destination": "/etc/ilogtail/checkpoint",
			"Source": "/var/lib/kube-system-logtail-ds/checkpoint"
		}, {
			"Destination": "/etc/ilogtail/config",
			"Source": "/var/lib/kube-system-logtail-ds/config"
		}, {
			"Destination": "/etc/ilogtail/instance_config",
			"Source": "/var/lib/kube-system-logtail-ds/instance_config"
		}, {
			"Destination": "/dev/termination-log",
			"Source": "/var/lib/kubelet/pods/4991ae55-8a3c-4228-9668-4d4feb748ad1/containers/loongcollector/f6f3d3b1"
		}, {
			"Destination": "/etc/hosts",
			"Source": "/var/lib/kubelet/pods/4991ae55-8a3c-4228-9668-4d4feb748ad1/etc-hosts"
		}, {
			"Destination": "/usr/local/ilogtail/apsara_log_conf.json",
			"Source": "/var/lib/kubelet/pods/4991ae55-8a3c-4228-9668-4d4feb748ad1/volume-subpaths/log-config/loongcollector/8"
		}, {
			"Destination": "/usr/local/ilogtail/ilogtail_config.json",
			"Source": "/var/lib/kubelet/pods/4991ae55-8a3c-4228-9668-4d4feb748ad1/volume-subpaths/loongcollector-config/loongcollector/5"
		}, {
			"Destination": "/etc/init.d/loongcollectord",
			"Source": "/var/lib/kubelet/pods/4991ae55-8a3c-4228-9668-4d4feb748ad1/volume-subpaths/loongcollectord/loongcollector/9"
		}, {
			"Destination": "/var/run/secrets/kubernetes.io/serviceaccount",
			"Source": "/var/lib/kubelet/pods/4991ae55-8a3c-4228-9668-4d4feb748ad1/volumes/kubernetes.io~projected/kube-api-access-zp49x"
		}, {
			"Destination": "/var/addon",
			"Source": "/var/lib/kubelet/pods/4991ae55-8a3c-4228-9668-4d4feb748ad1/volumes/kubernetes.io~secret/addon-token"
		}, {
			"Destination": "/var/run",
			"Source": "/var/run"
		}],
		"Path": "/logtail_host/var/lib/docker/overlay2/04eb45c706a8136171b3fb68e242c8fd19a7e053e2262fada30890d1b326cf3c/diff/usr/local/ilogtail/self_metrics",
		"Tags": [],
		"UpperDir": "/var/lib/docker/overlay2/04eb45c706a8136171b3fb68e242c8fd19a7e053e2262fada30890d1b326cf3c/diff"
	}]
})");
        ContainerManager containerManager;
        containerManager.UpdateAllContainers();
        EXPECT_EQ(containerManager.mContainerMap.size(), 1);
    }
}

void ContainerManagerUnittest::TestUpdateDiffContainers() const {
    {
        // test empty diff containers meta
        LogtailPluginMock::GetInstance()->SetUpDiffContainersMeta("");
        ContainerManager containerManager;
        containerManager.UpdateDiffContainers();
        EXPECT_EQ(containerManager.mContainerMap.size(), 0);
        EXPECT_EQ(containerManager.mStoppedContainerIDs.size(), 0);
    }
    {
        // test diff containers meta
        LogtailPluginMock::GetInstance()->SetUpDiffContainersMeta(R"({
            "DiffCmd": {
                "Update": [
                    {
                        "ID": "123",
                        "UpperDir": "/var/lib/docker/containers/123",
                        "LogPath": "/var/lib/docker/containers/123/logs"
                    },
                    {
                        "ID": "1234",
                        "UpperDir": "/var/lib/docker/containers/1234",
                        "LogPath": "/var/lib/docker/containers/1234/logs"
                    }
                ],
                "Delete": [123],
                "Stop": ["123"]
            }
        })");
        ContainerManager containerManager;
        containerManager.UpdateDiffContainers();
        EXPECT_EQ(containerManager.mContainerMap.size(), 1);
        EXPECT_EQ(containerManager.mStoppedContainerIDs.size(), 1);
        EXPECT_EQ(containerManager.mStoppedContainerIDs[0], "123");
    }
}

UNIT_TEST_CASE(ContainerManagerUnittest, TestGetMatchedContainersInfo)
UNIT_TEST_CASE(ContainerManagerUnittest, TestUpdateAllContainers)
UNIT_TEST_CASE(ContainerManagerUnittest, TestUpdateDiffContainers)

} // namespace logtail

UNIT_TEST_MAIN
