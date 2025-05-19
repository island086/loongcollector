#include "container_manager/ContainerManager.h"
#include "file_server/ContainerInfo.h"
#include "file_server/FileServer.h"
#include <boost/regex.hpp>


namespace logtail {
    void ContainerManager::UpdateAllContainers() {
        ContainerInfo containerInfo;
        std::string errorMsg;
        if (!ContainerInfo::ParseByJSONObj(paramsJSON, containerInfo, errorMsg)) {

        }
    }


    void CheckContainerUpdate() {
        auto nameConfigMap = FileServer::GetInstance()->GetAllFileDiscoveryConfigs();
        for (auto itr = nameConfigMap.begin(); itr != nameConfigMap.end(); ++itr) {
            const FileDiscoveryOptions* options = itr->second.first;
            if (options->IsContainerDiscoveryEnabled()) {
                auto containerOptions = options->GetContainerDiscoveryOptions()
            }
        }
    }


    void CheckConfigContainerUpdate(ContainerDiscoveryOptions options, std::vector<ContainerInfo> containers) {
        
    } 


    


    std::tuple<int, int, std::vector<std::string>, std::vector<std::string>> getAllAcceptedInfoV2(
        std::unordered_map<std::string, bool>& fullList,
        std::unordered_map<std::string, std::shared_ptr<DockerInfoDetail>>& matchList,
        const std::unordered_map<std::string, std::string>& includeLabel,
        const std::unordered_map<std::string, std::string>& excludeLabel,
        const std::unordered_map<std::string, boost::regex>& includeLabelRegex,
        const std::unordered_map<std::string, boost::regex>& excludeLabelRegex,
        const std::unordered_map<std::string, std::string>& includeEnv,
        const std::unordered_map<std::string, std::string>& excludeEnv,
        const std::unordered_map<std::string, boost::regex>& includeEnvRegex,
        const std::unordered_map<std::string, boost::regex>& excludeEnvRegex,
        const K8SFilter* k8sFilter
    ) {
        std::unique_lock<std::shared_mutex> lockGuard(lock);
        
        std::vector<std::string> matchAddedList;
        std::vector<std::string> matchDeletedList;
        int newCount = 0;
        int delCount = 0;

        // 移除已删除的容器
        for (auto it = fullList.begin(); it != fullList.end();) {
            if (containerMap.find(it->first) == containerMap.end()) {
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
            if (auto it = containerMap.find(id); it != containerMap.end()) {
                // 更新为最新的 info
                info = it->second;
            } else {
                std::cerr << "Matched container not in Docker center: " << id << std::endl;
            }
        }

        // 添加新容器
        for (const auto& [id, info] : containerMap) {
            // 如果 fullList 中不存在该 id
            if (fullList.find(id) == fullList.end()) {
                fullList[id] = true; // 加入到 fullList

                // 检查标签和环境匹配
                if (isContainerLabelMatch(includeLabel, excludeLabel, includeLabelRegex, excludeLabelRegex, info) &&
                    isContainerEnvMatch(includeEnv, excludeEnv, includeEnvRegex, excludeEnvRegex, info.get()) &&
                    info->K8SInfo.IsMatch(k8sFilter)) {
                    newCount++;
                    matchList[id] = info; // 添加到匹配列表
                    matchAddedList.push_back(id); // 添加到变换列表
                }
            }
        }

        return {newCount, delCount, matchAddedList, matchDeletedList}; // 返回元组
    }



    bool isMapLabelsMatch(
    const std::unordered_map<std::string, std::string>& includeLabel,
    const std::unordered_map<std::string, std::string>& excludeLabel,
    const std::unordered_map<std::string, boost::regex>& includeLabelRegex,
    const std::unordered_map<std::string, boost::regex>& excludeLabelRegex,
    const std::unordered_map<std::string, std::string>& labels) {

    if (!includeLabel.empty() || !includeLabelRegex.empty()) {
        bool matchedFlag = false;

        // 检查静态 include 标签
        for (const auto& [key, val] : includeLabel) {
            auto it = labels.find(key);
            if (it != labels.end() && (val.empty() || it->second == val)) {
                matchedFlag = true;
                break;
            }
        }

        // 如果匹配，则不需要检查正则表达式
        if (!matchedFlag) {
            for (const auto& [key, reg] : includeLabelRegex) {
                auto it = labels.find(key);
                if (it != labels.end() && boost::regex_match(it->second, reg)) {
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
    for (const auto& [key, val] : excludeLabel) {
        auto it = labels.find(key);
        if (it != labels.end() && (val.empty() || it->second == val)) {
            return false;
        }
    }

    // 检查 exclude 正则
    for (const auto& [key, reg] : excludeLabelRegex) {
        auto it = labels.find(key);
        if (it != labels.end() && boost::regex_match(it->second, reg)) {
            return false;
        }
    }

    return true;
}

bool isContainerLabelMatch(
    const std::unordered_map<std::string, std::string>& includeLabel,
    const std::unordered_map<std::string, std::string>& excludeLabel,
    const std::unordered_map<std::string, boost::regex>& includeLabelRegex,
    const std::unordered_map<std::string, boost::regex>& excludeLabelRegex,
    const DockerInfoDetail& info) {

    return isMapLabelsMatch(includeLabel, excludeLabel, includeLabelRegex, excludeLabelRegex, info.ContainerInfo.Config.Labels);
}

bool isMathEnvItem(
    const std::string& env,
    const std::unordered_map<std::string, std::string>& staticEnv,
    const std::unordered_map<std::string, boost::regex>& regexEnv) {

    std::string envKey, envValue;
    size_t pos = env.find('=');
    if (pos != std::string::npos) {
        envKey = env.substr(0, pos);
        envValue = env.substr(pos + 1);
    } else {
        envKey = env;
    }

    // 检查静态环境变量
    if (!staticEnv.empty()) {
        auto it = staticEnv.find(envKey);
        if (it != staticEnv.end() && (it->second.empty() || it->second == envValue)) {
            return true;
        }
    }

    // 检查正则表达式环境变量
    if (!regexEnv.empty()) {
        auto it = regexEnv.find(envKey);
        if (it != regexEnv.end() && boost::regex_match(envValue, it->second)) {
            return true;
        }
    }

    return false;
}

bool isContainerEnvMatch(
    const std::unordered_map<std::string, std::string>& includeEnv,
    const std::unordered_map<std::string, std::string>& excludeEnv,
    const std::unordered_map<std::string, boost::regex>& includeEnvRegex,
    const std::unordered_map<std::string, boost::regex>& excludeEnvRegex,
    const DockerInfoDetail& info) {

    if (!includeEnv.empty() || !includeEnvRegex.empty()) {
        bool matchFlag = false;

        // 遍历每一个环境变量
        for (const auto& env : info.ContainerInfo.Config.Env) {
            if (isMathEnvItem(env, includeEnv, includeEnvRegex)) {
                matchFlag = true;
                break;
            }
        }
        // 如果没有匹配，返回 false
        if (!matchFlag) {
            return false;
        }
    }

    // 检查 exclude 环境变量
    if (!excludeEnv.empty() || !excludeEnvRegex.empty()) {
        for (const auto& env : info.ContainerInfo.Config.Env) {
            if (isMathEnvItem(env, excludeEnv, excludeEnvRegex)) {
                return false;
            }
        }
    }

    return true;
}

// K8SInfo 的 innerMatch 方法实现
bool k8SInfoMatch(const K8SFilter& filter, K8sInfo& k8sInfo) {
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
    if (k8sInfo.mLabels.empty()) {
        return isMapLabelsMatch(filter.mIncludeLabels, filter.mExcludeLabels, filter.mIncludeLabelRegs, filter.mExcludeLabelRegs, k8sInfo.mLabels);
    }
    
    return isMapLabelsMatch(filter.mIncludeLabels, filter.mExcludeLabels, filter.mIncludeLabelRegs, filter.mExcludeLabelRegs, k8sInfo.mLabels);
}
}