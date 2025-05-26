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
    
}

UNIT_TEST_CASE(ContainerManagerUnittest, TestGetAllAcceptedInfoV2)

} // namespace logtail

UNIT_TEST_MAIN
