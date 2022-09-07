#include <iostream>

#include <cstdlib>
#include "gtest/gtest.h"
#include "pbrb.h"
#include "pmem_engine.h"

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
  IndexerT indexer;
  uint32_t maxPageNum = 100;
  PBRB pbrb(maxPageNum, &timestamp, &indexer, &sUMap);

  struct schema1Value {
    uint16_t field1;
    uint16_t field2;
  } __attribute__((packed));
  schema1Value v1{1, 2};

  auto sid1 = schema1.schemaId;
  Key k1 = {.schemaId = sid1};
  std::string pk1_expected("\001\0", 2);
  auto pk1 = k1.generatePK(v1.field1);
  ASSERT_EQ(pk1, pk1_expected);

  ValuePtr *vp1 = new ValuePtr;
  vp1->timestamp = timestamp;
  vp1->addr.pmemAddr = 0x12345678;
  vp1->isHot = false;
  indexer.insert({k1, vp1});

  std::string info[2] = {"Read k1, Cache k1(Cold)", "Read k1 (hot)"};
  // Step 1: Read k1, Cache k1(Cold)
  // Step 2: Read k1 (hot)
  for (int step = 1; step <= 2; step++) {
    NKV_LOG_I(std::cout, "Step {}: {}", step, info[step - 1]);
    IndexerIterator idxIter = indexer.find(k1);
    ASSERT_NE(idxIter, indexer.end());
    if (idxIter != indexer.end()) {
      ValuePtr *vPtr = idxIter->second;
      if (step == 1) ASSERT_EQ(vPtr->isHot, false);
      if (step == 2) ASSERT_EQ(vPtr->isHot, true);
      if (vPtr->isHot) {
        // Read PBRB
        TimeStamp ts_step2;
        ts_step2.getNow();
        Value read_result;
        bool status = pbrb.read(vPtr->timestamp, ts_step2, vPtr->addr.pbrbAddr,
                                k1.schemaId, read_result);
        ASSERT_EQ(status, true);
        schema1Value rv1;
        memcpy(&rv1, read_result.data(), sizeof(rv1));
        NKV_LOG_I(std::cout,
                  "Read K1 [schemaId: {}, primaryKey: {}], Value1 [field1: {}, "
                  "field2: {}]",
                  k1.schemaId, k1.primaryKey, (uint16_t)rv1.field1,
                  (uint16_t)rv1.field2);
        ASSERT_EQ(rv1.field1, v1.field1);
        ASSERT_EQ(rv1.field2, v1.field2);
      } else {
        // Read PLog get a value
        Value pbrb_v1 = Value((const char *)(&v1), 4);
        TimeStamp ts_step1;
        ts_step1.getNow();
        bool status = pbrb.write(vPtr->timestamp, ts_step1, k1.schemaId,
                                 pbrb_v1, idxIter);
        ASSERT_EQ(status, true);
        ASSERT_EQ(vPtr->isHot, true);
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
                  9999999),  // MAX_SIZE: 1 << 20 (1048576)
  };
  ASSERT_EQ(fields[0].size, 8);
  ASSERT_EQ(fields[1].size, 8);
  ASSERT_EQ(fields[2].size, 8);
  ASSERT_EQ(fields[3].size, 128);
  ASSERT_EQ(fields[4].size, 128);
  ASSERT_EQ(fields[5].size, 1024);
  ASSERT_EQ(fields[6].size, 1048576);

  SchemaAllocator sa;
  Schema schema = sa.createSchema("Schema", 0, fields);
  ASSERT_EQ(schema.size, 8 + 8 + 8 + 128 + 128 + 1024 + 1048576);
}

}  // namespace NKV
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
