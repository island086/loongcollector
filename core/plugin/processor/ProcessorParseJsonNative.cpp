/*
 * Copyright 2023 iLogtail Authors
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

#include "plugin/processor/ProcessorParseJsonNative.h"

#include "rapidjson/document.h"
#if defined(__EXCLUDE_SSE4_2__)
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#else
#include <plugin/processor/simdjson.h>
#endif

#include "collection_pipeline/plugin/instance/ProcessorInstance.h"
#include "common/ParamExtractor.h"
#include "models/LogEvent.h"
#include "monitor/metric_constants/MetricConstants.h"

namespace logtail {

#if defined(__EXCLUDE_SSE4_2__)
static std::string RapidjsonValueToString(const rapidjson::Value& value) {
    if (value.IsString())
        return std::string(value.GetString(), value.GetStringLength());
    else if (value.IsBool())
        return ToString(value.GetBool());
    else if (value.IsInt())
        return ToString(value.GetInt());
    else if (value.IsUint())
        return ToString(value.GetUint());
    else if (value.IsInt64())
        return ToString(value.GetInt64());
    else if (value.IsUint64())
        return ToString(value.GetUint64());
    else if (value.IsDouble())
        return ToString(value.GetDouble());
    else if (value.IsNull())
        return "";
    else // if (value.IsObject() || value.IsArray())
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        value.Accept(writer);
        return std::string(buffer.GetString(), buffer.GetLength());
    }
}
#endif

const std::string ProcessorParseJsonNative::sName = "processor_parse_json_native";

bool ProcessorParseJsonNative::Init(const Json::Value& config) {
    std::string errorMsg;

    // SourceKey
    if (!GetMandatoryStringParam(config, "SourceKey", mSourceKey, errorMsg)) {
        PARAM_ERROR_RETURN(mContext->GetLogger(),
                           mContext->GetAlarm(),
                           errorMsg,
                           sName,
                           mContext->GetConfigName(),
                           mContext->GetProjectName(),
                           mContext->GetLogstoreName(),
                           mContext->GetRegion());
    }

    if (!mCommonParserOptions.Init(config, *mContext, sName)) {
        return false;
    }

#if !defined(__EXCLUDE_SSE4_2__)
    auto my_implementation = simdjson::get_available_implementations()["westmere"];
    if (! my_implementation) { exit(1); }
    if (! my_implementation->supported_by_runtime_system()) { exit(1); }
    simdjson::get_active_implementation() = my_implementation;
    LOG_INFO(sLogger, ("simdjson active implementation : ", simdjson::get_active_implementation()->name()));
#else
    LOG_INFO(sLogger, ("active implementation : ", "rapidjson"));
#endif

    mDiscardedEventsTotal = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_DISCARDED_EVENTS_TOTAL);
    mOutFailedEventsTotal = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_OUT_FAILED_EVENTS_TOTAL);
    mOutKeyNotFoundEventsTotal = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_OUT_KEY_NOT_FOUND_EVENTS_TOTAL);
    mOutSuccessfulEventsTotal = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_OUT_SUCCESSFUL_EVENTS_TOTAL);

    return true;
}


void ProcessorParseJsonNative::Process(PipelineEventGroup& logGroup) {
    if (logGroup.GetEvents().empty()) {
        return;
    }

    const StringView& logPath = logGroup.GetMetadata(EventGroupMetaKey::LOG_FILE_PATH_RESOLVED);
    EventsContainer& events = logGroup.MutableEvents();

    size_t wIdx = 0;
    for (size_t rIdx = 0; rIdx < events.size(); ++rIdx) {
        if (ProcessEvent(logPath, events[rIdx], logGroup.GetAllMetadata())) {
            if (wIdx != rIdx) {
                events[wIdx] = std::move(events[rIdx]);
            }
            ++wIdx;
        }
    }
    events.resize(wIdx);
}

bool ProcessorParseJsonNative::ProcessEvent(const StringView& logPath,
                                            PipelineEventPtr& e,
                                            const GroupMetadata& metadata) {
    if (!IsSupportedEvent(e)) {
        ADD_COUNTER(mOutFailedEventsTotal, 1);
        return true;
    }
    auto& sourceEvent = e.Cast<LogEvent>();
    if (!sourceEvent.HasContent(mSourceKey)) {
        ADD_COUNTER(mOutKeyNotFoundEventsTotal, 1);
        return true;
    }

    auto rawContent = sourceEvent.GetContent(mSourceKey);

    bool sourceKeyOverwritten = false;
    bool parseSuccess = JsonLogLineParser(sourceEvent, logPath, e, sourceKeyOverwritten);

    if (!parseSuccess || !sourceKeyOverwritten) {
        sourceEvent.DelContent(mSourceKey);
    }
    if (mCommonParserOptions.ShouldAddSourceContent(parseSuccess)) {
        AddLog(mCommonParserOptions.mRenamedSourceKey, rawContent, sourceEvent, false);
    }
    if (mCommonParserOptions.ShouldAddLegacyUnmatchedRawLog(parseSuccess)) {
        AddLog(mCommonParserOptions.legacyUnmatchedRawLogKey, rawContent, sourceEvent, false);
    }
    if (mCommonParserOptions.ShouldEraseEvent(parseSuccess, sourceEvent, metadata)) {
        ADD_COUNTER(mDiscardedEventsTotal, 1);
        return false;
    }
    ADD_COUNTER(mOutSuccessfulEventsTotal, 1);
    return true;
}


#if defined(__EXCLUDE_SSE4_2__)
bool ProcessorParseJsonNative::JsonLogLineParser(LogEvent& sourceEvent,
                                                 const StringView& logPath,
                                                 PipelineEventPtr& e,
                                                 bool& sourceKeyOverwritten) {
    StringView buffer = sourceEvent.GetContent(mSourceKey);

    if (buffer.empty())
        return false;

    bool parseSuccess = true;
    rapidjson::Document doc;
    doc.Parse(buffer.data(), buffer.size());
    if (doc.HasParseError()) {
        if (AlarmManager::GetInstance()->IsLowLevelAlarmValid()) {
            LOG_WARNING(sLogger,
                        ("parse json log fail, log", buffer)("rapidjson offset", doc.GetErrorOffset())(
                            "rapidjson error", doc.GetParseError())("project", GetContext().GetProjectName())(
                            "logstore", GetContext().GetLogstoreName())("file", logPath));
            AlarmManager::GetInstance()->SendAlarmWarning(PARSE_LOG_FAIL_ALARM,
                                                          std::string("parse json fail:") + buffer.to_string(),
                                                          GetContext().GetRegion(),
                                                          GetContext().GetProjectName(),
                                                          GetContext().GetConfigName(),
                                                          GetContext().GetLogstoreName());
        }
        ADD_COUNTER(mOutFailedEventsTotal, 1);
        parseSuccess = false;
    } else if (!doc.IsObject()) {
        if (AlarmManager::GetInstance()->IsLowLevelAlarmValid()) {
            LOG_WARNING(sLogger,
                        ("invalid json object, log", buffer)("project", GetContext().GetProjectName())(
                            "logstore", GetContext().GetLogstoreName())("file", logPath));
            AlarmManager::GetInstance()->SendAlarmWarning(PARSE_LOG_FAIL_ALARM,
                                                          std::string("invalid json object:") + buffer.to_string(),
                                                          GetContext().GetRegion(),
                                                          GetContext().GetProjectName(),
                                                          GetContext().GetConfigName(),
                                                          GetContext().GetLogstoreName());
        }
        ADD_COUNTER(mOutFailedEventsTotal, 1);
        parseSuccess = false;
    }
    if (!parseSuccess) {
        return false;
    }

    for (rapidjson::Value::ConstMemberIterator itr = doc.MemberBegin(); itr != doc.MemberEnd(); ++itr) {
        std::string contentKey = RapidjsonValueToString(itr->name);
        std::string contentValue = RapidjsonValueToString(itr->value);

        StringBuffer contentKeyBuffer = sourceEvent.GetSourceBuffer()->CopyString(contentKey);
        StringBuffer contentValueBuffer = sourceEvent.GetSourceBuffer()->CopyString(contentValue);

        if (contentKey.c_str() == mSourceKey) {
            sourceKeyOverwritten = true;
        }

        AddLog(StringView(contentKeyBuffer.data, contentKeyBuffer.size),
               StringView(contentValueBuffer.data, contentValueBuffer.size),
               sourceEvent);
    }
    return true;
}
#else



bool ProcessorParseJsonNative::JsonLogLineParser(LogEvent& sourceEvent,
                                                 const StringView& logPath,
                                                 PipelineEventPtr& e,
                                                 bool& sourceKeyOverwritten) {
    StringView buffer = sourceEvent.GetContent(mSourceKey);

    if (buffer.empty())
        return false;

    simdjson::ondemand::parser parser;
    simdjson::padded_string bufStr(buffer.data(), buffer.size());
    simdjson::ondemand::document doc;
    
    auto error = parser.iterate(bufStr).get(doc);

    if (error) {
        if (AlarmManager::GetInstance()->IsLowLevelAlarmValid()) {
            LOG_WARNING(sLogger,
                        ("parse json log fail, log", buffer)("simdjson error", simdjson::simdjson_error(error).what())("project", GetContext().GetProjectName())(
                            "logstore", GetContext().GetLogstoreName())("file", logPath));
            AlarmManager::GetInstance()->SendAlarmWarning(PARSE_LOG_FAIL_ALARM,
                                                            std::string("parse json fail:") + buffer.to_string(),
                                                            GetContext().GetRegion(),
                                                            GetContext().GetProjectName(),
                                                            GetContext().GetConfigName(),
                                                            GetContext().GetLogstoreName());
        }
        ADD_COUNTER(mOutFailedEventsTotal, 1);
        return false;
    }
    simdjson::ondemand::object object;

    try {
        object = doc.get_object();
    } catch (simdjson::simdjson_error &error) {
        if (AlarmManager::GetInstance()->IsLowLevelAlarmValid()) {
            LOG_WARNING(sLogger,
                        ("parse json log fail, log", buffer)("simdjson error", error.what())("project", GetContext().GetProjectName())(
                            "logstore", GetContext().GetLogstoreName())("file", logPath));
            AlarmManager::GetInstance()->SendAlarmWarning(PARSE_LOG_FAIL_ALARM,
                                                            std::string("parse json fail:") + buffer.to_string(),
                                                            GetContext().GetRegion(),
                                                            GetContext().GetProjectName(),
                                                            GetContext().GetConfigName(),
                                                            GetContext().GetLogstoreName());
        }
        ADD_COUNTER(mOutFailedEventsTotal, 1);
        return false;
    }

    // Store parsed fields temporarily - reserve space to avoid reallocations
    std::vector<std::pair<StringView, StringView>> tempFields;
    tempFields.reserve(16); // Reserve space for typical JSON object size
    
    for(auto field : object) {
        try {
        // parses and writes out the key, after unescaping it,
        // to a string buffer. It causes a performance penalty.
        std::string_view keyv = field.unescaped_key();
        StringBuffer contentKeyBuffer = sourceEvent.GetSourceBuffer()->CopyString(keyv.data(), keyv.size());

        simdjson::ondemand::value value = field.value();
        std::string_view value_view;
        {
            // 获取 value 的字符串视图，减少字符串复制
            switch (value.type()) {
                case simdjson::ondemand::json_type::null:
                case simdjson::ondemand::json_type::number:
                case simdjson::ondemand::json_type::boolean: {
                    // Use raw_json for primitive types to avoid conversion overhead
                    value_view = value.raw_json();
                    break;
                }
                case simdjson::ondemand::json_type::string: {
                    value_view = value.get_string();
                    break;
                }
                case simdjson::ondemand::json_type::object:
                case simdjson::ondemand::json_type::array: {
                    value_view = simdjson::to_json_string(value);
                    break;
                }
                default: {
                    static const std::string_view unknown_type = "unknown type";
                    value_view = unknown_type;
                    break;
                }
            }
        }

        StringBuffer contentValueBuffer = sourceEvent.GetSourceBuffer()->CopyString(value_view.data(), value_view.size());   
        if (keyv == mSourceKey) {
            sourceKeyOverwritten = true;
        }
        // Store temporarily instead of adding directly
        tempFields.emplace_back(StringView(contentKeyBuffer.data, contentKeyBuffer.size),
                               StringView(contentValueBuffer.data, contentValueBuffer.size));
        } catch (simdjson::simdjson_error &error) {
            if (AlarmManager::GetInstance()->IsLowLevelAlarmValid()) {
                LOG_WARNING(sLogger,
                            ("parse json log fail, log", buffer)("simdjson error", error.what())("project", GetContext().GetProjectName())(
                                "logstore", GetContext().GetLogstoreName())("file", logPath));
                AlarmManager::GetInstance()->SendAlarmWarning(PARSE_LOG_FAIL_ALARM,
                                                                std::string("parse json fail:") + buffer.to_string(),
                                                                GetContext().GetRegion(),
                                                                GetContext().GetProjectName(),
                                                                GetContext().GetConfigName(),
                                                                GetContext().GetLogstoreName());
            }
              ADD_COUNTER(mOutFailedEventsTotal, 1);
              return false;
        }
    }
    // Only add fields if all parsing succeeded
    for (const auto& field : tempFields) {
        AddLog(field.first, field.second, sourceEvent);
    }
    return true;
}
#endif



void ProcessorParseJsonNative::AddLog(const StringView& key,
                                      const StringView& value,
                                      LogEvent& targetEvent,
                                      bool overwritten) {
    if (!overwritten && targetEvent.HasContent(key)) {
        return;
    }
    targetEvent.SetContentNoCopy(key, value);
}

bool ProcessorParseJsonNative::IsSupportedEvent(const PipelineEventPtr& e) const {
    return e.Is<LogEvent>();
}

} // namespace logtail
