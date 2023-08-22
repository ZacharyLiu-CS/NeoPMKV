//
//  pbrb_test.cc
//
//  Created by zhenliu on 09/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//
#include <climits>
#include <iostream>

#include <cstdlib>
#include <memory>
#include <random>
#include "gtest/gtest.h"
#include "mempool.h"
#include "pbrb.h"
#include "pmem_engine.h"
#include "profiler.h"
#include "schema.h"
#include "schema_parser.h"

namespace NKV {

class PBRBTest : public testing::Test {
 private:
  SchemaAllocator _schemaAllocator;
  SchemaParserMap _sParser;
  SchemaUMap _sUMap;
  MemPool *_memPoolPtr = nullptr;
  PBRB *_pbrbPtr = nullptr;
  IndexerList _indexerList;
  uint32_t maxPageNum = 100;
  uint32_t pageSize = 4096;
  std::vector<SchemaField> fields1 = {
      SchemaField(FieldType::STRING, "field1", 6),
      SchemaField(FieldType::STRING, "field2", 6)};

  std::vector<SchemaField> fields2{SchemaField(FieldType::INT64T, "pk"),
                                   SchemaField(FieldType::STRING, "f1", 8),
                                   SchemaField(FieldType::STRING, "f2", 16)};

 public:
  void SetUp() override {
    _memPoolPtr = new MemPool(pageSize, maxPageNum);
    TimeStamp timestamp;
    timestamp.getNow();

    _pbrbPtr = new PBRB(maxPageNum, &timestamp, &_indexerList, &_sUMap,
                        &_sParser, nullptr, 60, 5, false);
  }
  void TearDown() override { Clear(); }

  void CreateSchema1() {
    Schema schema1 = _schemaAllocator.CreateSchema("schema1", 0, fields1);
    _sUMap.addSchema(schema1);
    _pbrbPtr->createCacheForSchema(schema1.getSchemaId(), schema1.getVersion());
    _indexerList.insert({schema1.getSchemaId(), std::make_shared<IndexerT>()});
    SchemaParser *parser = new SchemaParser(_memPoolPtr);
    _sParser.insert({schema1.getSchemaId(), parser});
  }
  void CreateSchema2() {
    Schema schema2 = _schemaAllocator.CreateSchema("schema1", 0, fields2);
    _sUMap.addSchema(schema2);
    _pbrbPtr->createCacheForSchema(schema2.getSchemaId(), schema2.getVersion());
    _indexerList.insert({schema2.getSchemaId(), std::make_shared<IndexerT>()});
    SchemaParser *parser = new SchemaParser(_memPoolPtr);
    _sParser.insert({schema2.getSchemaId(), parser});
  }

  void Clear() {
    _schemaAllocator.clear();
    _sUMap.clear();
    _sParser.clear();
    delete _pbrbPtr;
    _indexerList.clear();
    for (auto &[schemaid, parserPtr] : _sParser) {
      delete parserPtr;
    }
    delete _memPoolPtr;
  }

  Value BuildFieldValue(uint32_t i, uint32_t field, uint32_t size) {
    std::string num_str = std::to_string(i + field);
    int zero_padding = size - num_str.size();
    Value v;
    v.append(zero_padding, '0').append(num_str);
    return v;
  };

  Key BuildKey(uint32_t key, SchemaId sid) { return Key(sid, key); }

  Value BuildSchema1Value(uint32_t i, uint32_t seed, SchemaId schemaid = 1) {
    std::vector<Value> value;
    value.push_back(BuildFieldValue(i + seed, 0, 6));
    value.push_back(BuildFieldValue(i + seed, 1, 6));
    return _sParser[schemaid]->ParseFromUserWriteToSeq(_sUMap.find(schemaid),
                                                       value);
  }
  Value BuildSchema2Value(uint32_t i, uint32_t seed, SchemaId schemaid = 2) {
    std::vector<Value> value;
    value.push_back(BuildFieldValue(i + seed, 0, 8));
    value.push_back(BuildFieldValue(i + seed, 1, 8));
    value.push_back(BuildFieldValue(i + seed, 2, 16));

    return _sParser[schemaid]->ParseFromUserWriteToSeq(_sUMap.find(schemaid),
                                                       value);
  }
  void Insert(uint32_t key, SchemaId sid) {
    auto indexer = _indexerList[sid];
    ValuePtr vp1 = ValuePtr();
    vp1.setFullColdPmemAddr((PmemAddress)&vp1);
    indexer->insert({key, vp1});
  }
  bool CacheData(uint32_t key, uint32_t seed, SchemaId sid) {
    auto indexer = _indexerList[sid];
    auto iter = indexer->find(key);
    ValuePtr *vPtr = &iter->second;
    TimeStamp ts_step;
    ts_step.getNow();
    Value data =
        sid == 1 ? BuildSchema1Value(key, seed) : BuildSchema2Value(key, seed);
    // std::cout << "insert data: " << data << std::endl;
    bool status =
        _pbrbPtr->write(vPtr->getTimestamp(), ts_step, sid, data, iter);

    // std::cout << "status: " << status << std::endl;
    return status;
  }
  bool Read(uint32_t key, SchemaId sid, Value &readValue) {
    auto indexer = _indexerList[sid];
    auto iter = indexer->find(key);
    ValuePtr *vPtr = &iter->second;
    bool hotness = vPtr->isHot();
    TimeStamp ts_step;
    ts_step.getNow();

    if (hotness == false) {
      _pbrbPtr->schemaMiss(sid);
      return hotness;
    }
    _pbrbPtr->schemaHit(sid);
    bool status = _pbrbPtr->read(vPtr->getTimestamp(), ts_step,
                                 vPtr->getPBRBAddr(), sid, readValue, vPtr);
    // std::cout << "get value: " << readValue << std::endl;
    // std::cout << "status:" << status << std::endl;
    return hotness;
  }
};
TEST_F(PBRBTest, TestReadAfterInsert) {
  CreateSchema1();
  CreateSchema2();
  uint32_t itemCount = 1 << 10;
  uint32_t seed = 1452;
  SchemaId sid1 = 1;
  SchemaId sid2 = 2;
  for (uint32_t i = 0; i < itemCount; i++) {
    Insert(i, sid1);
    Insert(i, sid2);
  }
  for (uint32_t i = 0; i < itemCount; i++) {
    Value readValue1;
    bool s1 = Read(i, sid1, readValue1);
    EXPECT_EQ(s1, false);
    bool c1 = CacheData(i, seed, sid1);
    EXPECT_EQ(c1, true);

    Value readValue2;
    bool s2 = Read(i, sid2, readValue2);
    EXPECT_EQ(s1, false);
    bool c2 = CacheData(i, seed, sid2);
    EXPECT_EQ(c2, true);
  }
}
TEST_F(PBRBTest, TestReadAfterRead) {
  CreateSchema1();
  CreateSchema2();
  uint32_t itemCount = 1 << 10;
  uint32_t seed = 1452;
  SchemaId sid1 = 1;
  SchemaId sid2 = 2;

  for (uint32_t i = 0; i < itemCount; i++) {
    Insert(i, sid1);
    Insert(i, sid2);
  }
  for (uint32_t i = 0; i < itemCount; i++) {
    bool c1 = CacheData(i, seed, sid1);
    EXPECT_EQ(c1, true);

    bool c2 = CacheData(i, seed, sid2);
    EXPECT_EQ(c2, true);
  }
  for (uint32_t i = 0; i < itemCount; i++) {
    Value readValue1;
    Value expectValue1 = BuildSchema1Value(i, seed);
    bool s1 = Read(i, sid1, readValue1);
    EXPECT_EQ(s1, true);
    EXPECT_EQ(readValue1, expectValue1);

    Value readValue2;
    Value expectValue2 = BuildSchema2Value(i, seed);
    bool s2 = Read(i, sid2, readValue2);
    EXPECT_EQ(s1, true);
    EXPECT_EQ(readValue2, expectValue2);
  }
}

}  // namespace NKV
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
