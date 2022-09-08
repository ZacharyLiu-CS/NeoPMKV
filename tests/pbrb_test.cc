#include <iostream>

#include <cstdlib>
#include <random>
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
  Key k1(sid1, v1.field1);
  std::string pk1_expected("\001\0", 2);
  ASSERT_EQ(k1.primaryKey, pk1_expected);

  ValuePtr *vp1 = new ValuePtr;
  vp1->updatePmemAddr((PmemAddress)0x12345678);
  indexer.insert({k1, *vp1});

  std::string info[2] = {"Read k1, Cache k1(Cold)", "Read k1 (hot)"};
  // Step 1: Read k1, Cache k1(Cold)
  // Step 2: Read k1 (hot)
  for (int step = 1; step <= 2; step++) {
    NKV_LOG_I(std::cout, "Step {}: {}", step, info[step - 1]);
    IndexerIterator idxIter = indexer.find(k1);
    ASSERT_NE(idxIter, indexer.end());
    if (idxIter != indexer.end()) {
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

TEST(PBRBTest, Test02) {
  // Create Schema
  SchemaAllocator schemaAllocator;
  SchemaUMap sUMap;
  std::vector<SchemaField> s02Fields{SchemaField(FieldType::INT64T, "pk"),
                                     SchemaField(FieldType::STRING, "f1", 8),
                                     SchemaField(FieldType::STRING, "f2", 16)};
  Schema schema02 = schemaAllocator.createSchema("schema02", 0, s02Fields);
  sUMap.addSchema(schema02);

  // Generate KVs
  std::vector<Value> values;
  uint32_t length = 300;

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
    v.append(pk).append(f1).append(f2);
    ASSERT_EQ(v.size(), 32);
    values.push_back(v);
  }

  for (auto v : values) {
    uint64_t pk = *(uint64_t *)(v.substr(0, 8).data());
    Value f1 = v.substr(8, 8);
    Value f2 = v.substr(16, 16);
    NKV_LOG_D(std::cout, "pk: {}, f1: {}, f2: {}", pk, f1, f2);
  }

  // Generate access pattern
  enum class AccType : uint8_t { GET, PUT };

  struct Access {
    AccType accType;
    Key key;
    Value value;
  };

  // Create Indexer
  IndexerT indexer;
  for (auto v : values) {
    uint64_t pk = *(uint64_t *)(v.substr(0, 8).data());
    TimeStamp ts;
    ts.getNow();
    ValuePtr vPtr;
    vPtr.updatePmemAddr((PmemAddress)pk);
    indexer.insert({Key(schema02.schemaId, pk), vPtr});
  }

  // Create PBRB
  TimeStamp ts_start_pbrb;
  ts_start_pbrb.getNow();
  uint32_t maxPageNum = 256;
  PBRB pbrb(maxPageNum, &ts_start_pbrb, &indexer, &sUMap);

  // Cache all KVs
  for (int i = 0; i < 3; i++) {
    for (uint64_t pk = 1; pk <= length; pk++) {
      Key key(schema02.schemaId, pk);
      IndexerIterator idxIter = indexer.find(key);
      if (idxIter != indexer.end()) {
        ValuePtr *vPtr = &idxIter->second;
        if (vPtr->isHot()) {
          // Read PBRB
          TimeStamp tsRead;
          tsRead.getNow();
          Value read_result;
          bool status = pbrb.read(vPtr->getTimestamp(), tsRead, vPtr->getPBRBAddr(),
                                  key.schemaId, read_result, vPtr);
          ASSERT_EQ(status, true);
          ASSERT_EQ(read_result, values[pk - 1]);
        } else {
          // Read PLog get a value
          TimeStamp tsInsert;
          tsInsert.getNow();
          bool status = pbrb.syncwrite(vPtr->getTimestamp(), tsInsert, key.schemaId,
                                       values[pk - 1], idxIter);
          ASSERT_EQ(status, true);
          ASSERT_EQ(vPtr->isHot(), true);
        }
      }
    }
  }
}

/*
TEST(PBRBTest, Test03) {
  // Create Schema
  SchemaAllocator schemaAllocator;
  SchemaUMap sUMap;
  std::vector<SchemaField> s02Fields{SchemaField(FieldType::INT64T, "pk"),
                                     SchemaField(FieldType::STRING, "f1", 8),
                                     SchemaField(FieldType::STRING, "f2", 16)};
  Schema schema03 = schemaAllocator.createSchema("schema02", 0, s02Fields);
  sUMap.addSchema(schema03);

  // Generate KVs
  std::vector<Value> values;
  uint32_t kvNumber = 300;

  for (uint64_t i = 1; i <= kvNumber; i++) {
    char buf[32];
    std::string pk((char *)&i, sizeof(i));
    memset(buf, 0, sizeof(buf));
    sprintf(buf, "f1.%04lu", i);
    std::string f1(buf, 8);
    memset(buf, 0, sizeof(buf));
    sprintf(buf, "f2.%08lu", i);
    std::string f2(buf, 16);

    Value v;
    v.append(pk).append(f1).append(f2);
    ASSERT_EQ(v.size(), 32);
    values.push_back(v);
  }

  for (auto v : values) {
    uint64_t pk = *(uint64_t *)(v.substr(0, 8).data());
    Value f1 = v.substr(8, 8);
    Value f2 = v.substr(16, 16);
    NKV_LOG_I(std::cout, "pk: {}, f1: {}, f2: {}", pk, f1, f2);
  }

  // Create Indexer
  IndexerT indexer;
  for (auto v : values) {
    uint64_t pk = *(uint64_t *)(v.substr(0, 8).data());
    TimeStamp ts;
    ts.getNow();
    ValuePtr *vPtr = new ValuePtr;
    vPtr->addr.pmemAddr = (PmemAddress)pk;
    vPtr->isHot = false;
    vPtr->timestamp = ts;
    indexer.insert({Key(schema03.schemaId, pk), vPtr});
  }

  // Generate access pattern
  enum class AccType : uint8_t { GET, PUT };

  struct Access {
    AccType accType;
    Key key;
    Value value;
  };

  std::vector<Access> accessVec;
  uint32_t accessCount = 1000;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distrib(1, kvNumber);

  for (int i = 0; i < accessCount; i++) {
    uint64_t k = distrib(gen);
    accessVec.push_back(Access{.accType = AccType::GET,
                               .key = Key(schema03.schemaId, k)});
  }
  // Create PBRB
  TimeStamp ts_start_pbrb;
  ts_start_pbrb.getNow();
  uint32_t maxPageNum = 256;
  PBRB pbrb(maxPageNum, &ts_start_pbrb, &indexer, &sUMap);

  // Cache all KVs
  for (int i = 0; i < 3; i++) {
    for (uint64_t pk = 1; pk <= kvNumber; pk++) {
      Key key(schema03.schemaId, pk);
      IndexerIterator idxIter = indexer.find(key);
      if (idxIter != indexer.end()) {
        ValuePtr *vPtr = idxIter->second;
        if (vPtr->isHot) {
          // Read PBRB
          TimeStamp tsRead;
          tsRead.getNow();
          Value read_result;
          bool status = pbrb.read(vPtr->timestamp, tsRead, vPtr->addr.pbrbAddr,
                                  key.schemaId, read_result, vPtr);
          ASSERT_EQ(status, true);
          ASSERT_EQ(read_result, values[pk - 1]);
        } else {
          // Read PLog get a value
          TimeStamp tsInsert;
          tsInsert.getNow();
          bool status = pbrb.write(vPtr->timestamp, tsInsert, key.schemaId,
                                   values[pk - 1], idxIter);
          ASSERT_EQ(status, true);
          ASSERT_EQ(vPtr->isHot, true);
        }
      }
    }
  }

  // Release vPtrs
  for (auto iter : indexer) {
    if (iter.second != nullptr) delete iter.second;
  }
}
*/
}  // namespace NKV
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
