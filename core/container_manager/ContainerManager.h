#pragma once

#include <string>
#include <unordered_map>
#include <vector>


namespace logtail {

class ContainerManager {
public:
    struct DockerInfoDetail {
        struct ContainerInfo {
            struct Config {
                std::unordered_map<std::string, std::string> Labels; // 标签映射
                std::vector<std::string> Env; // 环境变量
            } Config;
        } ContainerInfo;
    };

    void UpdateAllContainers();

private:
    std::unordered_map<std::string, std::string> mContainers;
    uint32_t mLastUpdateTime = 0;

};

}