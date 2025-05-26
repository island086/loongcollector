#include "container_manager/ContainerManager.h"
#include "file_server/FileServer.h"
#include <boost/regex.hpp>


namespace logtail {
    void ContainerManager::UpdateAllContainers() {
        
    }


    void CheckContainerUpdate() {
        auto nameConfigMap = FileServer::GetInstance()->GetAllFileDiscoveryConfigs();
        for (auto itr = nameConfigMap.begin(); itr != nameConfigMap.end(); ++itr) {
            const FileDiscoveryOptions* options = itr->second.first;
            if (options->IsContainerDiscoveryEnabled()) {
                auto containerOptions = options->GetContainerDiscoveryOptions();
            }
        }
    }


    void CheckConfigContainerUpdate(ContainerDiscoveryOptions options, std::vector<ContainerInfo> containers) {
        
    } 




bool IsMapLabelsMatch(const MatchCriteriaFilter& filter, const std::unordered_map<std::string, std::string>& labels) {
    if (!filter.mIncludeFields.mFieldsMap.empty() || !filter.mIncludeFields.mFieldsRegMap.empty()) {
        bool matchedFlag = false;

        // 检查静态 include 标签
        for (const auto& [key, val] : filter.mIncludeFields.mFieldsMap) {
            auto it = labels.find(key);
            if (it != labels.end() && (val.empty() || it->second == val)) {
                matchedFlag = true;
                break;
            }
        }

        // 如果匹配，则不需要检查正则表达式
        if (!matchedFlag) {
            for (const auto& [key, reg] : filter.mIncludeFields.mFieldsRegMap) {
                auto it = labels.find(key);
                if (it != labels.end() && boost::regex_match(it->second, *reg)) {
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
    for (const auto& [key, val] : filter.mExcludeFields.mFieldsMap) {
        auto it = labels.find(key);
        if (it != labels.end() && (val.empty() || it->second == val)) {
            return false;
        }
    }

    // 检查 exclude 正则
    for (const auto& [key, reg] : filter.mExcludeFields.mFieldsRegMap) {
        auto it = labels.find(key);
        if (it != labels.end() && boost::regex_match(it->second, *reg)) {
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
        std::unordered_map<std::string, bool>& fullList,
        std::unordered_map<std::string, std::shared_ptr<RawContainerInfo>>& matchList,
        const std::unordered_map<std::string, std::string>& includeLabel,
        const ContainerFilters& filters
    ) {
        std::vector<std::string> matchAddedList;
        std::vector<std::string> matchDeletedList;
        int newCount = 0;
        int delCount = 0;

        // 移除已删除的容器
        for (auto it = fullList.begin(); it != fullList.end();) {
            if (mContainerMap.find(it->first) == mContainerMap.end()) {
                const std::string& id = it->first;
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
        for (auto& [id, info] : matchList) {
            if (auto it = mContainerMap.find(id); it != mContainerMap.end()) {
                // 更新为最新的 info
                info = it->second;
            } else {
                std::cerr << "Matched container not in Docker center: " << id << std::endl;
            }
        }

        // 添加新容器
        for (const auto& [id, info] : mContainerMap) {
            // 如果 fullList 中不存在该 id
            if (fullList.find(id) == fullList.end()) {
                fullList[id] = true; // 加入到 fullList

                // 检查标签和环境匹配
                if (IsMapLabelsMatch(filters.mContainerLabelFilter, info->mContainerLabels) &&
                    IsMapLabelsMatch(filters.mEnvFilter, info->mEnv) &&
                    IsK8sFilterMatch(filters.mK8SFilter, info->mK8sInfo)) {
                    newCount++;
                    matchList[id] = info; // 添加到匹配列表
                    matchAddedList.push_back(id); // 添加到变换列表
                }
            }
        }
    }
}