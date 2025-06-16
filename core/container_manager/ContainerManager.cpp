#include "container_manager/ContainerManager.h"
#include "file_server/FileServer.h"
#include <boost/regex.hpp>
#include "go_pipeline/LogtailPlugin.h"
#include "json/json.h"
#include "common/JsonUtil.h"





namespace logtail {

    ContainerManager::ContainerManager() {
        LOG_INFO(sLogger, ("ContainerManager", "init"));
        mThread = CreateThread([this]() { Run(); });
    }
    ContainerManager::~ContainerManager() = default;


    void ContainerManager::Run() {
        while (true) {
            UpdateAllContainers();
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }
    void ContainerManager::UpdateAllContainers() {
        std::string allContainersMeta = LogtailPlugin::GetInstance()->GetAllContainersMeta();
        LOG_INFO(sLogger, ("allContainersMeta", allContainersMeta));
         // cmd 解析json
        Json::Value jsonParams;
        std::string errorMsg;
        if (!ParseJsonTable(allContainersMeta, jsonParams, errorMsg)) {
            LOG_ERROR(sLogger, ("invalid docker container params", allContainersMeta)("errorMsg", errorMsg));
            return;
        }
        std::unordered_map<std::string, std::shared_ptr<ContainerInfo>> tmpContainerMap; 
        Json::Value allContainers = jsonParams["AllCmd"];
        for (const auto& container : allContainers) {
            std::string containerId = container["ID"].asString();
            std::string containerUpperDir = container["UpperDir"].asString();
            std::string containerLogPath = container["LogPath"].asString();

            std::shared_ptr<ContainerInfo> containerInfo = std::make_shared<ContainerInfo>();
            containerInfo->mID = containerId;
            containerInfo->mUpperDir = containerUpperDir;
            containerInfo->mLogPath = containerLogPath;
            tmpContainerMap[containerId] = containerInfo;
        }
        mContainerMapMutex.lock();
        mContainerMap = tmpContainerMap;
        mContainerMapMutex.unlock();
    }


    void ContainerManager::CheckConfigContainerUpdate(const FileDiscoveryOptions* options) {
        //GetAllAcceptedInfoV2
        std::unordered_map<std::string, std::shared_ptr<ContainerInfo>> containerInfoMap;
        auto containerInfos = options->GetContainerInfo();
        if (containerInfos) {
            for (const auto& info : *containerInfos) {
                containerInfoMap[info.mID] = std::make_shared<ContainerInfo>(info);
            }
        }
        GetAllAcceptedInfoV2(*(options->GetFullContainerList()), containerInfoMap, options->GetContainerDiscoveryOptions().mContainerFilters);
    } 

    void ContainerManager::CheckContainerUpdate() {
        auto nameConfigMap = FileServer::GetInstance()->GetAllFileDiscoveryConfigs();
        for (auto itr = nameConfigMap.begin(); itr != nameConfigMap.end(); ++itr) {
            const FileDiscoveryOptions* options = itr->second.first;
            if (options->IsContainerDiscoveryEnabled()) {
                //auto containerOptions = options->GetContainerDiscoveryOptions();
                CheckConfigContainerUpdate(options);
            }
        }
    }


bool IsMapLabelsMatch(const MatchCriteriaFilter& filter, const std::unordered_map<std::string, std::string>& labels) {
    if (!filter.mIncludeFields.mFieldsMap.empty() || !filter.mIncludeFields.mFieldsRegMap.empty()) {
        bool matchedFlag = false;

        // 检查静态 include 标签
        for (const auto& pair : filter.mIncludeFields.mFieldsMap) {
            auto it = labels.find(pair.first);
            if (it != labels.end() && (pair.second.empty() || it->second == pair.second)) {
                matchedFlag = true;
                break;
            }
        }

        // 如果匹配，则不需要检查正则表达式
        if (!matchedFlag) {
            for (const auto& pair : filter.mIncludeFields.mFieldsRegMap) {
                auto it = labels.find(pair.first);
                if (it != labels.end() && boost::regex_match(it->second, *pair.second)) {
                    matchedFlag = true;
                    break;
                }
            }
        }

        // 如果没有匹配，返回 false
        if (!matchedFlag) {
            return false;
        }
    }

    // 检查 exclude 标签
    for (const auto& pair : filter.mExcludeFields.mFieldsMap) {
        auto it = labels.find(pair.first);
        if (it != labels.end() && (pair.second.empty() || it->second == pair.second)) {
            return false;
        }
    }

    // 检查 exclude 正则
    for (const auto& pair : filter.mExcludeFields.mFieldsRegMap) {
        auto it = labels.find(pair.first);
        if (it != labels.end() && boost::regex_match(it->second, *pair.second)) {
            return false;
        }
    }

    return true;
}


bool IsK8sFilterMatch(const K8sFilter& filter, const K8sInfo& k8sInfo) {
    // 匹配命名空间
    if (filter.mNamespaceReg && !boost::regex_match(k8sInfo.mNamespace, *filter.mNamespaceReg)) {
        return false;
    }
    // 匹配 Pod 名称
    if (filter.mPodReg && !boost::regex_match(k8sInfo.mPod, *filter.mPodReg)) {
        return false;
    }
    // 匹配容器名称
    if (filter.mContainerReg && !boost::regex_match(k8sInfo.mContainerName, *filter.mContainerReg)) {
        return false;
    }
    
    // 确保 Labels 不为 nullptr，使用默认的空映射初始化
    return IsMapLabelsMatch(filter.mK8sLabelFilter, k8sInfo.mLabels);
}


void ContainerManager::GetAllAcceptedInfoV2(
        std::set<std::string>& fullList,
        std::unordered_map<std::string, std::shared_ptr<ContainerInfo>>& matchList,
        const ContainerFilters& filters
    ) {
        std::vector<std::string> matchAddedList;
        std::vector<std::string> matchDeletedList;
        int newCount = 0;
        int delCount = 0;

        // 移除已删除的容器
        for (auto it = fullList.begin(); it != fullList.end();) {
            if (mContainerMap.find(*it) == mContainerMap.end()) {
                const std::string& id = *it;
                it = fullList.erase(it); // 删除元素并移到下一个
                if (matchList.find(id) != matchList.end()) {
                    matchDeletedList.push_back(id);
                    matchList.erase(id);
                    delCount++;
                }
            } else {
                ++it;
            }
        }

        // 更新匹配的容器状态
        for (auto& pair : matchList) {
            if (auto it = mContainerMap.find(pair.first); it != mContainerMap.end()) {
                // 更新为最新的 info
                pair.second = it->second;
            } else {
                std::cerr << "Matched container not in Docker center: " << pair.first << std::endl;
            }
        }

        // 添加新容器
        for (const auto& pair : mContainerMap) {
            // 如果 fullList 中不存在该 id
            if (fullList.find(pair.first) == fullList.end()) {
                fullList.insert(pair.first); // 加入到 fullList
                // 检查标签和环境匹配
                if (IsMapLabelsMatch(filters.mContainerLabelFilter, pair.second->mContainerLabels) &&
                    IsMapLabelsMatch(filters.mEnvFilter, pair.second->mEnv) &&
                    IsK8sFilterMatch(filters.mK8SFilter, pair.second->mK8sInfo)) {
                    newCount++;
                    matchList[pair.first] = pair.second; // 添加到匹配列表
                    matchAddedList.push_back(pair.first); // 添加到变换列表
                }
            }
        }
    }
}