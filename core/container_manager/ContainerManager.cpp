#include "container_manager/ContainerManager.h"
#include <ctime>
#include "ConfigManager.h"
#include "file_server/FileServer.h"
#include <boost/regex.hpp>
#include "go_pipeline/LogtailPlugin.h"
#include "json/json.h"
#include "common/JsonUtil.h"





namespace logtail {

    ContainerManager::ContainerManager() = default;
    
    ContainerManager::~ContainerManager() = default;

    ContainerManager* ContainerManager::GetInstance() {
        static ContainerManager instance;
        return &instance;
    }

    void ContainerManager::Init() {
        if (mIsRunning) {
            return;
        }
        mIsRunning = true;
        LOG_INFO(sLogger, ("ContainerManager", "init"));
        mThread = CreateThread([this]() { Run(); });
    }


    void ContainerManager::Run() {
        time_t lastUpdateAllTime = 0;
        time_t lastUpdateDiffTime = 0;
        while (true) {
            time_t now = time(nullptr);
            if (now - lastUpdateAllTime >= 100) {
                UpdateAllContainers();
                lastUpdateAllTime = now;
            } else if (now - lastUpdateDiffTime >= 10) {
                UpdateDiffContainers();
                lastUpdateDiffTime = now;
            } 
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }

    void ContainerManager::DoUpdateContainerPaths() {
        auto nameConfigMap = FileServer::GetInstance()->GetAllFileDiscoveryConfigs();
        for (auto& pair : mConfigContainerDiffMap) {
            const auto& itr = nameConfigMap.find(pair.first);
            if (itr == nameConfigMap.end()) {
                continue;
            }
            const auto& options = itr->second.first;
            const auto& ctx = itr->second.second;


            const auto& diff = pair.second;
            for (const auto& container : diff->mAdded) {
                options->UpdateRawContainerInfo(container, ctx);
            }
            for (const auto& container : diff->mModified) {
                options->UpdateRawContainerInfo(container, ctx);
            }
        }
        mConfigContainerDiffMap.clear();
    }


    bool ContainerManager::CheckContainerUpdate() {
        bool isUpdate = false;
        auto nameConfigMap = FileServer::GetInstance()->GetAllFileDiscoveryConfigs();
        for (auto itr = nameConfigMap.begin(); itr != nameConfigMap.end(); ++itr) {
            FileDiscoveryOptions* options = itr->second.first;
            if (options->IsContainerDiscoveryEnabled()) {
                bool isCurrentConfigUpdate = CheckConfigContainerUpdate(options, itr->second.second);
                if (isCurrentConfigUpdate) {
                    isUpdate = true;
                }
            }
        }
        return isUpdate;
    }

    bool ContainerManager::CheckConfigContainerUpdate(FileDiscoveryOptions* options, const CollectionPipelineContext* ctx) {
        std::unordered_map<std::string, std::shared_ptr<RawContainerInfo>> containerInfoMap;
        const auto& containerInfos = options->GetContainerInfo();
        if (containerInfos) {
            for (const auto& info : *containerInfos) {
                containerInfoMap[info.mID] = info.mRawContainerInfo;
            }
        }
        std::vector<std::string> removedList;
        std::vector<std::string> matchAddedList;
        ContainerDiff diff;
        GetMatchedContainersInfo(*(options->GetFullContainerList()), diff, containerInfoMap, options->GetContainerDiscoveryOptions().mContainerFilters);
        if (diff.IsEmpty()) {
            return false;
        }
        mConfigContainerDiffMap[ctx->GetConfigName()] = std::make_shared<ContainerDiff>(diff);
        return true;
    } 

    void ContainerManager::UpdateDiffContainers() {
        std::string diffContainersMeta = LogtailPlugin::GetInstance()->GetDiffContainersMeta();
        Json::Value jsonParams;
        std::string errorMsg;
        if (!ParseJsonTable(diffContainersMeta, jsonParams, errorMsg)) {
            LOG_ERROR(sLogger, ("invalid docker container params", diffContainersMeta)("errorMsg", errorMsg));
            return;
        }
        Json::Value diffContainers = jsonParams["DiffCmd"];
        Json::Value updateContainers = diffContainers["Update"];
        Json::Value deleteContainers = diffContainers["Delete"];
        Json::Value stopContainers = diffContainers["Stop"];

        for (const auto& container : updateContainers) {
            std::string containerId = container["ID"].asString();
            std::string containerUpperDir = container["UpperDir"].asString();
            std::string containerLogPath = container["LogPath"].asString();

            std::shared_ptr<RawContainerInfo> containerInfo = std::make_shared<RawContainerInfo>();
            containerInfo->mID = containerId;
            containerInfo->mUpperDir = containerUpperDir;
            containerInfo->mLogPath = containerLogPath;

            std::lock_guard<std::mutex> lock(mContainerMapMutex);
            mContainerMap[containerId] = containerInfo;
        }

        for (const auto& container : deleteContainers) {
            std::string containerId = container.asString();
            std::lock_guard<std::mutex> lock(mContainerMapMutex);
            mContainerMap.erase(containerId);
        }
        for (const auto& container : stopContainers) {
            std::string containerId = container.asString();
            mStoppedContainerIDsMutex.lock();
            mStoppedContainerIDs.push_back(containerId);
            mStoppedContainerIDsMutex.unlock();
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
        std::unordered_map<std::string, std::shared_ptr<RawContainerInfo>> tmpContainerMap; 
        Json::Value allContainers = jsonParams["AllCmd"];
        for (const auto& container : allContainers) {
            std::string containerId = container["ID"].asString();
            std::string containerUpperDir = container["UpperDir"].asString();
            std::string containerLogPath = container["LogPath"].asString();

            std::shared_ptr<RawContainerInfo> containerInfo = std::make_shared<RawContainerInfo>();
            containerInfo->mID = containerId;
            containerInfo->mUpperDir = containerUpperDir;
            containerInfo->mLogPath = containerLogPath;
            tmpContainerMap[containerId] = containerInfo;
        }
        std::lock_guard<std::mutex> lock(mContainerMapMutex);
        mContainerMap = tmpContainerMap;
    }
    

    void ContainerManager::GetContainerStoppedEvents(std::vector<Event*>& eventVec) {
        auto nameConfigMap = FileServer::GetInstance()->GetAllFileDiscoveryConfigs();
        mStoppedContainerIDsMutex.lock();
        std::vector<std::string> stoppedContainerIDs;
        stoppedContainerIDs.swap(mStoppedContainerIDs);
        mStoppedContainerIDsMutex.unlock();

        for (const auto& containerId : stoppedContainerIDs) {
            Event* pStoppedEvent = new Event(containerId, "", EVENT_ISDIR | EVENT_CONTAINER_STOPPED, -1, 0);
            for (auto itr = nameConfigMap.begin(); itr != nameConfigMap.end(); ++itr) {
                const FileDiscoveryOptions* options = itr->second.first;
                if (options->IsContainerDiscoveryEnabled()) {
                    const auto& containerInfos = options->GetContainerInfo();
                    if (containerInfos) {
                        for (auto& info : *containerInfos) {
                            if (info.mID == containerId) {
                                info.mStopped = true;
                                pStoppedEvent->SetConfigName(itr->first);
                                break;
                            }
                        }
                    }
                }
            }
            pStoppedEvent->SetContainerID(containerId);
            eventVec.push_back(pStoppedEvent);
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


void ContainerManager::GetMatchedContainersInfo(
        std::set<std::string>& fullList,
        ContainerDiff& diff,
        const std::unordered_map<std::string, std::shared_ptr<RawContainerInfo>>& matchList,
        const ContainerFilters& filters
    ) {
        int newCount = 0;
        int delCount = 0;

        // 移除已删除的容器
        for (auto it = fullList.begin(); it != fullList.end();) {
            if (mContainerMap.find(*it) == mContainerMap.end()) {
                const std::string& id = *it;
                it = fullList.erase(it); // 删除元素并移到下一个
                if (matchList.find(id) != matchList.end()) {
                    diff.mRemoved.push_back(id);
                    //matchList.erase(id);
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
                if (*pair.second != *it->second) {
                    diff.mModified.push_back(it->second);
                }
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
                    diff.mAdded.push_back(pair.second); // 添加到变换列表
                }
            }
        }
    }
}