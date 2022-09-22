#include <iostream>

#include <cstdlib>
#include <memory>
#include <random>
#include "gtest/gtest.h"
#include "pbrb.h"
#include "pmem_engine.h"
#include "profiler.h"

namespace NKV {
TEST(PBRBTest, Test01) {
  SchemaAllocator schemaAllocator;
  SchemaUMap sUMap;
  std::vector<SchemaField> fields = {SchemaField(FieldType::INT16T, "field1"),
                                     SchemaField(FieldType::INT16T, "field2")};
  Schema schema1 = schemaAllocator.createSchema("schema1", 0, fields);
  sUMap.addSchema(schema1);

  TimeStamp timestamp;
  timestamp.getNow();
  IndexerList indexerList;
  indexerList.insert({1, std::make_shared<IndexerT>()});
  auto indexer = indexerList[1];
  uint32_t maxPageNum = 100;
  PBRB pbrb(maxPageNum, &timestamp, &indexerList, &sUMap);

  struct schema1Value {
    uint16_t field1;
    uint16_t field2;
  } __attribute__((packed));
  schema1Value v1{1, 2};

  auto sid1 = schema1.schemaId;
  Key k1(sid1, v1.field1);
  uint64_t pk1_expected = v1.field1;
  ASSERT_EQ(k1.primaryKey, pk1_expected);

  ValuePtr *vp1 = new ValuePtr;
  vp1->updatePmemAddr((PmemAddress)0x12345678);
  indexer->insert({k1.primaryKey, *vp1});

  std::string info[2] = {"Read k1, Cache k1(Cold)", "Read k1 (hot)"};
  // Step 1: Read k1, Cache k1(Cold)
  // Step 2: Read k1 (hot)
  for (int step = 1; step <= 2; step++) {
    NKV_LOG_I(std::cout, "Step {}: {}", step, info[step - 1]);
    IndexerIterator idxIter = indexer->find(k1.primaryKey);
    ASSERT_NE(idxIter, indexer->end());
    if (idxIter != indexer->end()) {
      ValuePtr *vPtr = &idxIter->second;
      if (step == 1) ASSERT_EQ(vPtr->isHot(), false);
      if (step == 2) ASSERT_EQ(vPtr->isHot(), true);
      if (vPtr->isHot()) {
        // Read PBRB
        TimeStamp ts_step2;
        ts_step2.getNow();
        Value read_result;
        bool status =
            pbrb.read(vPtr->getTimestamp(), ts_step2, vPtr->getPBRBAddr(),
                      k1.schemaId, read_result, vPtr);
        ASSERT_EQ(status, true);
        schema1Value rv1;
        NKV_LOG_I(std::cout,
                  "Read K1 [schemaId: {}, primaryKey: {}], Value1 [field1: {}, "
                  "field2: {}]",
                  k1.schemaId, k1.primaryKey, (uint16_t)rv1.field1,
                  (uint16_t)rv1.field2);
        ASSERT_EQ(read_result, Value(12, '1'));
      } else {
        // Read PLog get a value
        Value pbrb_v1 = Value(12, '1');
        TimeStamp ts_step1;
        ts_step1.getNow();
        bool status = pbrb.syncwrite(vPtr->getTimestamp(), ts_step1,
                                     k1.schemaId, pbrb_v1, idxIter);
        ASSERT_EQ(status, true);
        ASSERT_EQ(vPtr->isHot(), true);
      }
    }
  }
  delete vp1;
}

TEST(SchemaTest, FieldTest) {
  SchemaAllocator schemaAllocator;
  SchemaUMap sUMap;
  std::vector<SchemaField> fields = {
      SchemaField(FieldType::INT64T, "pk"),
      SchemaField(FieldType::STRING, "field1", 7),    // 8
      SchemaField(FieldType::STRING, "field2", 8),    // 8
      SchemaField(FieldType::STRING, "field3", 107),  // 128
      SchemaField(FieldType::STRING, "field4", 128),  // 128
      SchemaField(FieldType::STRING, "field5", 999),  // 1024
      SchemaField(FieldType::STRING, "bigField",
                  9999),  // MAX_SIZE: 1 << 20 (1048576)
  };
  // ASSERT_EQ(fields[0].size, 8);
  // ASSERT_EQ(fields[1].size, 8);
  // ASSERT_EQ(fields[2].size, 8);
  // ASSERT_EQ(fields[3].size, 128);
  // ASSERT_EQ(fields[4].size, 128);
  // ASSERT_EQ(fields[5].size, 1024);
  // ASSERT_EQ(fields[6].size, 1048576);

  SchemaAllocator sa;
  Schema schema = sa.createSchema("Schema", 0, fields);
  // ASSERT_EQ(schema.size, 8 + 8 + 8 + 128 + 128 + 1024 + 1048576);
}

bool generateKV(uint32_t length, std::vector<Value> &values,
                IndexerList &indexerList) {
  // Generate KVs

  Value padding("0000", 4);
  padding.resize(4);
  PointProfiler generate_value;
  generate_value.start();
  for (uint64_t i = 1; i <= length; i++) {
    char buf[32];
    std::string pk((char *)&i, sizeof(i));
    memset(buf, 0, sizeof(buf));
    sprintf(buf, "f1.%04lu", i);
    std::string f1(buf, 8);
    memset(buf, 0, sizeof(buf));
    sprintf(buf, "f2.%08lu", i);
    std::string f2(buf, 16);

    Value v;
    v.append(padding)
        .append(pk)
        .append(padding)
        .append(f1)
        .append(padding)
        .append(f2);
    // ASSERT_EQ(v.size(), 44);
    values.emplace_back(v);
  }
  generate_value.end();
  NKV_LOG_I(std::cout, "Genenation Duration: {:.2f}",
            generate_value.duration() / (double) NANOSEC_BASE);

  // for (auto v : values) {
  //   uint64_t pk = *(uint64_t *)(v.substr(4, 8).data());
  //   Value f1 = v.substr(16, 8);
  //   Value f2 = v.substr(28, 16);
  //   NKV_LOG_D(std::cout, "pk: {}, f1: {}, f2: {}", pk, f1, f2);
  // }

  // Generate access pattern
  enum class AccType : uint8_t { GET, PUT };

  struct Access {
    AccType accType;
    Key key;
    Value value;
  };

  PointProfiler indexer_timer;
  indexer_timer.start();
  // Create Indexer
  indexerList.insert({1, std::make_shared<IndexerT>()});
  auto indexer = indexerList[1];
  for (auto v : values) {
    uint64_t pk = *(uint64_t *)(v.substr(4, 8).data());
    TimeStamp ts;
    ts.getNow();
    ValuePtr vPtr;
    vPtr.updatePmemAddr((PmemAddress)pk);
    indexer->insert({pk, vPtr});
  }
  indexer_timer.end();
  NKV_LOG_I(std::cout, "Indexer Insertion Duration: {:.2f}s",
            indexer_timer.duration() / (double) NANOSEC_BASE);
  return true;
}

TEST(PBRBTest, Test02) {
  // Create Schema
  SchemaAllocator schemaAllocator;
  SchemaUMap sUMap;
  std::vector<SchemaField> s02Fields{SchemaField(FieldType::INT64T, "pk"),
                                     SchemaField(FieldType::STRING, "f1", 8),
                                     SchemaField(FieldType::STRING, "f2", 16)};
  Schema schema02 = schemaAllocator.createSchema("schema02", 0, s02Fields);
  sUMap.addSchema(schema02);

  uint32_t length = 1 << 20;
  uint32_t maxPageNum = 1 << 14;
  std::vector<Value> values;
  IndexerList indexerList;
  generateKV(length, values, indexerList);
  // Create PBRB
  TimeStamp ts_start_pbrb;
  ts_start_pbrb.getNow();
  PBRB pbrb(maxPageNum, &ts_start_pbrb, &indexerList, &sUMap, 60);

  auto &indexer = indexerList[1];
  // Cache all KVs
  for (int i = 0; i < 3; i++) {
    PointProfiler timer;
    timer.start();
    for (uint64_t pk = 1; pk <= length; pk++) {
      Key key(schema02.schemaId, pk);
      IndexerIterator idxIter = indexer->find(key.primaryKey);
      if (idxIter != indexer->end()) {
        ValuePtr *vPtr = &idxIter->second;
        if (vPtr->isHot()) {
          // Read PBRB
          pbrb.schemaHit(schema02.schemaId);
          TimeStamp tsRead;
          tsRead.getNow();
          Value read_result;
          bool status =
              pbrb.read(vPtr->getTimestamp(), tsRead, vPtr->getPBRBAddr(),
                        key.schemaId, read_result, vPtr);
          ASSERT_EQ(status, true);
          ASSERT_EQ(read_result, values[pk - 1]);
        } else {
          // Read PLog get a value
          pbrb.schemaMiss(schema02.schemaId);
          TimeStamp tsInsert;
          tsInsert.getNow();
          bool status = pbrb.syncwrite(vPtr->getTimestamp(), tsInsert,
                                       key.schemaId, values[pk - 1], idxIter);
          ASSERT_EQ(status, true);
          ASSERT_EQ(vPtr->isHot(), true);
        }
      }
    }
    timer.end();
    NKV_LOG_I(std::cout, "Step {}: Duration: {:.2f}s", i + 1,
              timer.duration() / (double) NANOSEC_BASE);
  }
#ifdef ENABLE_BREAKDOWN
  pbrb.analyzePerf();
#endif
}

TEST(PBRBTest, Test03) {
  // Create Schema
  SchemaAllocator schemaAllocator;
  SchemaUMap sUMap;
  std::vector<SchemaField> s03Fields{SchemaField(FieldType::INT64T, "pk"),
                                     SchemaField(FieldType::STRING, "f1", 8),
                                     SchemaField(FieldType::STRING, "f2", 16)};
  Schema schema03 = schemaAllocator.createSchema("schema02", 0, s03Fields);
  sUMap.addSchema(schema03);

  uint32_t length = 20;
  uint32_t maxPageNum = 1 << 10;
  std::vector<Value> values;
  IndexerList indexerList;
  generateKV(length, values, indexerList);
  // Create PBRB
  TimeStamp ts_start_pbrb;
  ts_start_pbrb.getNow();
  PBRB pbrb(maxPageNum, &ts_start_pbrb, &indexerList, &sUMap, 5);

  auto &indexer = indexerList[1];
  std::vector<uint64_t> seq[] = {
      {1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}};
  std::vector<uint64_t> sleepTime{3, 3, 3};
  // Cache 0 ~ 4:
  for (int i = 0; i < 3; i++) {
    PointProfiler timer;
    timer.start();
    for (auto pk : seq[i]) {
      Key key(schema03.schemaId, pk);
      IndexerIterator idxIter = indexer->find(key.primaryKey);
      if (idxIter != indexer->end()) {
        ValuePtr *vPtr = &idxIter->second;
        if (vPtr->isHot()) {
          // Read PBRB
          NKV_LOG_I(std::cout, "[pk = {:4}] : [hit]", pk);
          pbrb.schemaHit(schema03.schemaId);
          TimeStamp tsRead;
          tsRead.getNow();
          Value read_result;
          bool status =
              pbrb.read(vPtr->getTimestamp(), tsRead, vPtr->getPBRBAddr(),
                        key.schemaId, read_result, vPtr);
          ASSERT_EQ(status, true);
          ASSERT_EQ(read_result, values[pk - 1]);
        } else {
          // Read PLog get a value
          pbrb.schemaMiss(schema03.schemaId);
          NKV_LOG_I(std::cout, "[pk = {:4}] : [miss]", pk);
          TimeStamp tsInsert;
          tsInsert.getNow();
          bool status = pbrb.syncwrite(vPtr->getTimestamp(), tsInsert,
                                       key.schemaId, values[pk - 1], idxIter);
          ASSERT_EQ(status, true);
          ASSERT_EQ(vPtr->isHot(), true);
        }
      }
    }
    sleep(sleepTime[i]);
    timer.end();
    NKV_LOG_I(std::cout, "Step {}: Duration: {:.2f}s (Sleeped {}s)", i + 1,
              timer.duration() / (double) NANOSEC_BASE, sleepTime[i]);
    // pbrb.traverseIdxGC();
  }
#ifdef ENABLE_BREAKDOWN
  pbrb.analyzePerf();
#endif
}

}  // namespace NKV
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
