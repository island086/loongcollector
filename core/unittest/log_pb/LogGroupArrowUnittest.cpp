
#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_union.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/options.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/util/checked_cast.h>

#include <chrono>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>

#include "LogFileReader.h"
#include "PipelineEventGroup.h"
#include "ProtocolConversion.h"
#include "SpanEvent.h"
#include "pipeline_event_group.pb.h"
#include "unittest/Unittest.h"

using namespace std;

namespace logtail {

class LogGroupArrowUnittest : public ::testing::Test {
public:
    void TestPBDeserialize();
    void TestArrowSerialize();
    void TestArrowDeserialize();
};

void LogGroupArrowUnittest::TestPBDeserialize() {
    ifstream file("protobuf_data-129.bin");
    if (!file) {
        std::cout << "无法打开文件: " << std::endl;
        return;
    }
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    std::cout << "content size: " << content.size() / 1024 << " KB" << std::endl;
    file.close();

    auto deserializationCost = 0;
    auto convertionCost = 0;
    auto loopCount = 100;
    for (size_t i = 0; i < loopCount; ++i) {
        auto start = chrono::high_resolution_clock::now();
        models::PipelineEventGroup pbGroup;
        pbGroup.ParseFromString(content);
        auto end1 = chrono::high_resolution_clock::now();
        auto buffer = make_unique<LogBuffer>();
        PipelineEventGroup group = PipelineEventGroup{std::shared_ptr<SourceBuffer>(std::move(buffer->sourcebuffer))};
        std::string errMsg;
        TransferPBToPipelineEventGroup(pbGroup, group, errMsg);
        auto end2 = chrono::high_resolution_clock::now();
        deserializationCost += chrono::duration_cast<chrono::microseconds>(end1 - start).count();
        convertionCost += chrono::duration_cast<chrono::microseconds>(end2 - end1).count();
        if (i == 0) {
            ofstream outFile("protobuf_data-129.json");
            if (!outFile) {
                std::cout << "无法打开文件: " << std::endl;
                return;
            }
            outFile << group.ToJsonString(true);
            outFile.close();
        }
    }
    std::cout << "Deserialization cost: " << deserializationCost / 1000
              << " ms, avg: " << deserializationCost / 1000 / loopCount << " ms" << std::endl;
    std::cout << "Conversion cost: " << convertionCost / 1000 << " ms, avg: " << convertionCost / 1000 / loopCount
              << " ms" << std::endl;
}

void GetTestPipelineEventGroup(PipelineEventGroup& group) {
    ifstream file("protobuf_data-129.bin");
    if (!file) {
        std::cout << "无法打开文件: " << std::endl;
        return;
    }
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    file.close();
    models::PipelineEventGroup pbGroup;
    pbGroup.ParseFromString(content);
    std::string errMsg;
    TransferPBToPipelineEventGroup(pbGroup, group, errMsg);
}

std::shared_ptr<arrow::Schema> CreatePipelineEventGroupSchema() {
    // SpanEvent 的 StructType
    auto span_event_type = arrow::struct_({
        arrow::field("Timestamp", arrow::uint64()),
        arrow::field("TraceID", arrow::binary()),
        arrow::field("SpanID", arrow::binary()),
        arrow::field("TraceState", arrow::binary()),
        arrow::field("ParentSpanID", arrow::binary()),
        arrow::field("Name", arrow::utf8()),
        arrow::field("Kind", arrow::int32()),
        arrow::field("StartTime", arrow::uint64()),
        arrow::field("EndTime", arrow::uint64()),
        arrow::field("Tags", arrow::map(arrow::utf8(), arrow::binary())),
    });

    // LogEvent 和 MetricEvent 类型（假设为空）
    auto log_event_type = arrow::struct_({});
    auto metric_event_type = arrow::struct_({});

    // 构建 Union 字段
    auto pipeline_events_type = arrow::sparse_union({arrow::field("Logs", arrow::list(log_event_type)),
                                                     arrow::field("Metrics", arrow::list(metric_event_type)),
                                                     arrow::field("Spans", arrow::list(span_event_type))},
                                                    {0, 1, 2}); // type_codes: 0=Logs, 1=Metrics, 2=Spans

    auto pipeline_event_group_type = arrow::struct_({
        arrow::field("Metadata", arrow::map(arrow::utf8(), arrow::binary())),
        arrow::field("Tags", arrow::map(arrow::utf8(), arrow::binary())),
        arrow::field("PipelineEvents", pipeline_events_type),
    });

    auto schema = std::make_shared<arrow::Schema>(
        std::vector<std::shared_ptr<arrow::Field>>({arrow::field("Group", pipeline_event_group_type)}));

    return schema;
}

void LogGroupArrowUnittest::TestArrowSerialize() {
    auto buffer = make_unique<LogBuffer>();
    PipelineEventGroup group = PipelineEventGroup{std::shared_ptr<SourceBuffer>(std::move(buffer->sourcebuffer))};
    GetTestPipelineEventGroup(group);

    arrow::MemoryPool* pool = arrow::default_memory_pool();
    auto schema = CreatePipelineEventGroupSchema();

    auto serializationCost = 0;
    auto loopCount = 1;
    for (size_t i = 0; i < loopCount; ++i) {
        auto start = chrono::high_resolution_clock::now();
        unique_ptr<arrow::ArrayBuilder> groupBuilder;

        arrow::Status status = arrow::MakeBuilder(pool, schema->field(0)->type(), &groupBuilder);
        if (!status.ok()) {
            std::cout << "Failed to create group builder: " << status.ToString() << std::endl;
            return;
        }
        auto* structBuilder = arrow::internal::checked_cast<arrow::StructBuilder*>(groupBuilder.get());
        status = structBuilder->Append();
        if (!status.ok()) {
            std::cout << "Failed to append to struct builder: " << status.ToString() << std::endl;
            return;
        }

        auto* metadataBuilder = arrow::internal::checked_cast<arrow::MapBuilder*>(groupBuilder->child(0));
        auto* tagsBuilder = arrow::internal::checked_cast<arrow::MapBuilder*>(groupBuilder->child(1));
        auto* pipelineEventsBuilder = arrow::internal::checked_cast<arrow::SparseUnionBuilder*>(groupBuilder->child(2));

        status = pipelineEventsBuilder->Append(2);
        if (!status.ok()) {
            std::cout << "Failed to append to union builder: " << status.ToString() << std::endl;
            return;
        }
        auto* spanEventsBuilder = arrow::internal::checked_cast<arrow::ListBuilder*>(pipelineEventsBuilder->child(2));
        status = spanEventsBuilder->Append();
        if (!status.ok()) {
            std::cout << "Failed to append to span events builder: " << status.ToString() << std::endl;
            return;
        }
        auto* spanBuilder = arrow::internal::checked_cast<arrow::StructBuilder*>(spanEventsBuilder->value_builder());
        for (const auto& event : group.GetEvents()) {
            const auto& span = event.Cast<SpanEvent>();
            status = spanBuilder->Append();
            if (!status.ok()) {
                std::cout << "Failed to append to span builder: " << status.ToString() << std::endl;
                return;
            }
            auto* timestamp_builder = arrow::internal::checked_cast<arrow::UInt64Builder*>(spanBuilder->child(0));
            auto* trace_id_builder = arrow::internal::checked_cast<arrow::BinaryBuilder*>(spanBuilder->child(1));
            auto* span_id_builder = arrow::internal::checked_cast<arrow::BinaryBuilder*>(spanBuilder->child(2));
            auto* trace_state_builder = arrow::internal::checked_cast<arrow::BinaryBuilder*>(spanBuilder->child(3));
            auto* parent_span_id_builder = arrow::internal::checked_cast<arrow::BinaryBuilder*>(spanBuilder->child(4));
            auto* name_builder = arrow::internal::checked_cast<arrow::StringBuilder*>(spanBuilder->child(5));
            auto* kind_builder = arrow::internal::checked_cast<arrow::Int32Builder*>(spanBuilder->child(6));
            auto* start_time_builder = arrow::internal::checked_cast<arrow::UInt64Builder*>(spanBuilder->child(7));
            auto* end_time_builder = arrow::internal::checked_cast<arrow::UInt64Builder*>(spanBuilder->child(8));
            auto* tags_builder = arrow::internal::checked_cast<arrow::MapBuilder*>(spanBuilder->child(9));

            status = timestamp_builder->Append(span.GetTimestamp());
            if (!status.ok()) {
                std::cout << "Failed to append to timestamp builder: " << status.ToString() << std::endl;
                return;
            }
            status = trace_id_builder->Append(span.GetTraceId().data());
            if (!status.ok()) {
                std::cout << "Failed to append to trace ID builder: " << status.ToString() << std::endl;
                return;
            }
            span_id_builder->Append(span.GetSpanId().data());
            trace_state_builder->Append(span.GetTraceState().data());
            parent_span_id_builder->Append(span.GetParentSpanId().data());
            std::ostringstream oss;
            for (const unsigned char c : span.GetParentSpanId()) {
                oss << "\\u" << std::hex << std::setw(4) << std::setfill('0') << static_cast<int>(c);
            }
            std::cout << "ParentSpanId: " << oss.str() << std::endl;

            name_builder->Append(span.GetName().data());
            kind_builder->Append(static_cast<int32_t>(span.GetKind()));
            start_time_builder->Append(span.GetStartTimeNs());
            std::cout << "StartTime: " << span.GetStartTimeNs() << std::endl;
            end_time_builder->Append(span.GetEndTimeNs());

            // Append tags;
            tags_builder->Append();
            auto key_builder = arrow::internal::checked_cast<arrow::StringBuilder*>(tags_builder->key_builder());
            auto val_builder = arrow::internal::checked_cast<arrow::BinaryBuilder*>(tags_builder->item_builder());
            for (const auto& tag : span.mTags.mInner) {
                key_builder->Append(tag.first.data());
                val_builder->Append(tag.second.data());
            }
        }
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        groupBuilder->Finish(&arrays.emplace_back());
        auto batch = arrow::RecordBatch::Make(schema, 1, arrays);
        auto result = arrow::ipc::SerializeRecordBatch(*batch, arrow::ipc::IpcWriteOptions::Defaults());
        if (!result.ok()) {
            std::cout << "Failed to serialize record batch: " << result.status().ToString() << std::endl;
            return;
        }
        auto buffer = result.ValueOrDie();
        auto output = std::string(reinterpret_cast<const char*>(buffer->data()), buffer->size());

        auto end1 = chrono::high_resolution_clock::now();
        serializationCost += chrono::duration_cast<chrono::microseconds>(end1 - start).count();
        if (i == 0) {
            std::cout << "content size: " << output.size() / 1024 << " KB" << std::endl;
            std::cout << "RecordBatch created with " << batch->num_rows() << " rows and " << batch->num_columns()
                      << " columns." << std::endl;
            ofstream outFile("arrow_data-129.bin");
            if (!outFile) {
                std::cout << "无法打开文件: " << std::endl;
                return;
            }
            outFile.write(output.data(), output.size());
            outFile.close();
        }
    }

    std::cout << "Serialization cost: " << serializationCost / 1000
              << " ms, avg: " << serializationCost / 1000 / loopCount << " ms" << std::endl;
}

void LogGroupArrowUnittest::TestArrowDeserialize() {
    ifstream file("arrow_data-129.bin", std::ios::binary);
    if (!file) {
        std::cout << "无法打开文件: " << std::endl;
        return;
    }
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    std::cout << "content size: " << content.size() / 1024 << " KB" << std::endl;
    file.close();

    auto schema = CreatePipelineEventGroupSchema();

    auto deserializationCost = 0;
    auto convertionCost = 0;
    auto loopCount = 100;
    for (size_t i = 0; i < loopCount; ++i) {
        auto start = chrono::high_resolution_clock::now();

        auto logBuffer = make_unique<LogBuffer>();
        PipelineEventGroup group
            = PipelineEventGroup{std::shared_ptr<SourceBuffer>(std::move(logBuffer->sourcebuffer))};
        auto buffer = arrow::Buffer::Wrap(content.data(), content.size());
        arrow::io::BufferReader input(buffer);
        arrow::ipc::DictionaryMemo memo;
        auto status = arrow::ipc::ReadRecordBatch(schema, &memo, arrow::ipc::IpcReadOptions::Defaults(), &input);
        if (!status.ok()) {
            std::cout << "Failed to read record batch: " << status.status().ToString() << std::endl;
            return;
        }

        auto batch = status.ValueOrDie();
        // 1. 获取 Group 字段（StructArray）
        auto group_array = std::dynamic_pointer_cast<arrow::StructArray>(batch->GetColumnByName("Group"));
        if (!group_array) {
            std::cout << "Group field is not a StructArray" << std::endl;
            return;
        }
        deserializationCost
            += chrono::duration_cast<chrono::microseconds>(chrono::high_resolution_clock::now() - start).count();

        // 2. 遍历每个 Group（假设只有一行）
        for (int i = 0; i < group_array->length(); ++i) {
            start = chrono::high_resolution_clock::now();
            auto metadata_array = std::dynamic_pointer_cast<arrow::MapArray>(group_array->GetFieldByName("Metadata"));

            // 4. 获取 Tags 字段（map<string, binary>）
            auto tags_array = std::dynamic_pointer_cast<arrow::MapArray>(group_array->GetFieldByName("Tags"));

            // 5. 获取 PipelineEvents 字段（假设为 List<Array>）
            auto groupStructArray = std::dynamic_pointer_cast<arrow::StructArray>(batch->GetColumnByName("Group"));
            if (!groupStructArray) {
                std::cout << "Group field is not a StructArray" << std::endl;
                return;
            }
            auto eventsUnionArray = std::dynamic_pointer_cast<arrow::SparseUnionArray>(
                groupStructArray->GetFieldByName("PipelineEvents"));
            if (!eventsUnionArray) {
                std::cout << "PipelineEvents field is not a SparseUnionArray" << std::endl;
                return;
            }
            auto eventsListArray = std::dynamic_pointer_cast<arrow::ListArray>(eventsUnionArray->field(2));
            if (!eventsListArray) {
                std::cout << "PipelineEvents field is not a ListArray" << std::endl;
                return;
            }
            auto eventsArray = std::dynamic_pointer_cast<arrow::StructArray>(eventsListArray->values());
            if (!eventsArray) {
                std::cout << "PipelineEvents field is not a StructArray" << std::endl;
                return;
            }
            // 提取 tags 字段（map<string, string>）
            auto tagsMapArray = std::dynamic_pointer_cast<arrow::MapArray>(eventsArray->GetFieldByName("Tags"));
            auto offsets = std::dynamic_pointer_cast<arrow::Int32Array>(tagsMapArray->offsets());
            auto keyArray = std::dynamic_pointer_cast<arrow::StringArray>(tagsMapArray->keys());
            auto valueArray = std::dynamic_pointer_cast<arrow::BinaryArray>(tagsMapArray->items());
            deserializationCost
                += chrono::duration_cast<chrono::microseconds>(chrono::high_resolution_clock::now() - start).count();

            for (int j = 0; j < eventsArray->length(); ++j) {
                start = chrono::high_resolution_clock::now();
                auto* event = group.AddSpanEvent();
                auto timestamp
                    = std::dynamic_pointer_cast<arrow::UInt64Array>(eventsArray->GetFieldByName("Timestamp"))->Value(j);
                auto traceID = std::dynamic_pointer_cast<arrow::BinaryArray>(eventsArray->GetFieldByName("TraceID"))
                                   ->GetString(j);
                auto spanID = std::dynamic_pointer_cast<arrow::BinaryArray>(eventsArray->GetFieldByName("SpanID"))
                                  ->GetString(j);
                auto traceState
                    = std::dynamic_pointer_cast<arrow::BinaryArray>(eventsArray->GetFieldByName("TraceState"))
                          ->GetString(j);
                auto parentSpanID
                    = std::dynamic_pointer_cast<arrow::BinaryArray>(eventsArray->GetFieldByName("ParentSpanID"))
                          ->GetString(j);
                auto name
                    = std::dynamic_pointer_cast<arrow::StringArray>(eventsArray->GetFieldByName("Name"))->GetString(j);
                auto kind = std::dynamic_pointer_cast<arrow::Int32Array>(eventsArray->GetFieldByName("Kind"))->Value(j);
                auto startTime
                    = std::dynamic_pointer_cast<arrow::UInt64Array>(eventsArray->GetFieldByName("StartTime"))->Value(j);
                auto endTime
                    = std::dynamic_pointer_cast<arrow::UInt64Array>(eventsArray->GetFieldByName("EndTime"))->Value(j);
                deserializationCost
                    += chrono::duration_cast<chrono::microseconds>(chrono::high_resolution_clock::now() - start)
                           .count();

                start = chrono::high_resolution_clock::now();
                event->SetTimestamp(timestamp, 0);
                event->SetTraceId(traceID);
                event->SetSpanId(spanID);
                event->SetTraceState(traceState);
                event->SetParentSpanId(parentSpanID);
                event->SetName(name);
                event->SetKind(SpanEvent::Kind(kind));
                event->SetStartTimeNs(startTime);
                event->SetEndTimeNs(endTime);
                int32_t offsetStart = offsets->Value(j);
                int32_t offsetEnd = offsets->Value(j + 1);
                for (int64_t k = offsetStart; k < offsetEnd; ++k) {
                    event->SetTag(keyArray->GetString(k), valueArray->GetString(k));
                }
                convertionCost
                    += chrono::duration_cast<chrono::microseconds>(chrono::high_resolution_clock::now() - start)
                           .count();
            }
        }
        if (i == 0) {
            ofstream outFile("arrow_data-129.json");
            if (!outFile) {
                std::cout << "无法打开文件: " << std::endl;
                return;
            }
            outFile << group.ToJsonString(true);
            outFile.close();
        }
    }
    std::cout << "Deserialization cost: " << deserializationCost / 1000
              << " ms, avg: " << deserializationCost / 1000 / loopCount << " ms" << std::endl;
    std::cout << "Conversion cost: " << convertionCost / 1000 << " ms, avg: " << convertionCost / 1000 / loopCount
              << " ms" << std::endl;
}

UNIT_TEST_CASE(LogGroupArrowUnittest, TestPBDeserialize)
UNIT_TEST_CASE(LogGroupArrowUnittest, TestArrowSerialize)
UNIT_TEST_CASE(LogGroupArrowUnittest, TestArrowDeserialize)

} // namespace logtail

UNIT_TEST_MAIN
