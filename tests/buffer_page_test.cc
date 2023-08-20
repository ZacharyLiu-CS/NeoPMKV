#include <iostream>

#include <cstdlib>
#include "gtest/gtest.h"
#include "pbrb.h"
#include "pmem_engine.h"

namespace NKV {
TEST(BufferPageTest, Initialization) {
  BufferPage *newPage = new BufferPage();
  newPage->initializePage();
  auto magic = newPage->getMagicPage();
  ASSERT_EQ(magic, 0x1010);
  auto schemaId = newPage->getSchemaIDPage();
  ASSERT_EQ(schemaId, 0);
  auto prevPage = newPage->getPrevPage();
  ASSERT_EQ(prevPage, nullptr);
  auto nextPage = newPage->getNextPage();
  ASSERT_EQ(nextPage, nullptr);
  auto hotNum = newPage->getHotRowsNumPage();
  ASSERT_EQ(hotNum, 0);
  delete newPage;
}

TEST(BufferPageTest, BasicFunctions) {
  BufferPage *pagePtr = new BufferPage();
  BufferPage *nextPtr = new BufferPage();
  BufferPage *prevPtr = new BufferPage();

  pagePtr->initializePage();
  pagePtr->setMagicPage(3579);
  ASSERT_EQ(pagePtr->getMagicPage(), 3579);
  pagePtr->setSchemaIDPage(2468);
  ASSERT_EQ(pagePtr->getSchemaIDPage(), 2468);
  pagePtr->setHotRowsNumPage(187);
  ASSERT_EQ(pagePtr->getHotRowsNumPage(), 187);

  pagePtr->initializePage();
  pagePtr->setPrevPage(prevPtr);
  ASSERT_EQ(pagePtr->getPrevPage(), prevPtr);
  pagePtr->setNextPage(nextPtr);
  ASSERT_EQ(pagePtr->getNextPage(), nextPtr);

  uint32_t rowSize = 32;
  uint32_t valueSize = rowSize - PBRB_ROW_HEADER_SIZE;
  uint32_t maxRowCnt = (pageSize - PBRB_PAGE_HEADER_SIZE) / rowSize;

  // Funtions: RowBitmap
  ASSERT_EQ(126, maxRowCnt);
  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt, 0, 8), 0);  // 00000000
  pagePtr->setRowBitMapPage(0);                             // 00000001
  ASSERT_EQ(pagePtr->isBitmapSet(0), true);
  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt, 0, 8), 1);  // 00000001
  pagePtr->setRowBitMapPage(1);
  pagePtr->setRowBitMapPage(3);
  ASSERT_EQ(pagePtr->setRowBitMapPage(3), false);
  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt), 2);
  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt, 2, 8), 2);
  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt, 3, 8), 4);
  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt, 0, 2), UINT32_MAX);
  pagePtr->clearRowBitMapPage(1);
  ASSERT_EQ(pagePtr->clearRowBitMapPage(1), false);
  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt), 1);
  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt, 0, 3), 1);
  // 120 ~ 124 (maxRowCnt == 125)
  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt, 120), 120);
  pagePtr->setRowBitMapPage(120);
  pagePtr->setRowBitMapPage(121);
  pagePtr->setRowBitMapPage(122);
  pagePtr->setRowBitMapPage(123);
  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt, 120), 124);
  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt, 120, 130), 124);
  pagePtr->setRowBitMapPage(124);
  pagePtr->setRowBitMapPage(125);

  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt, 120), UINT32_MAX);
  ASSERT_EQ(pagePtr->getFirstZeroBit(maxRowCnt, 120, 130), UINT32_MAX);

  // verify hot row count:
  ASSERT_EQ(pagePtr->getHotRowsNumPage(), 8);
  // 0, 3, 120, 121, 122, 123, 124, 125 is set.

  // Row header functions:
  RowOffset rOff;
  rOff = pagePtr->getFirstZeroBit(maxRowCnt);
  ASSERT_EQ(rOff, 1);
  pagePtr->setRowBitMapPage(rOff);
  RowAddr rAddr = pagePtr->_getRowAddr(rOff, rowSize);

  TimeStamp ts{.txn_ticks = 12345};
  pagePtr->setTimestampRow(rAddr, ts);
  ASSERT_NE(&(((RowHeader *)rAddr)->timestamp), &ts);
  ASSERT_EQ(pagePtr->getTimestampRow(rAddr).txn_ticks,
            ts.txn_ticks);

  pagePtr->setPlogAddrRow(rAddr, (PmemAddress)0x8765);
  ASSERT_EQ(pagePtr->getPlogAddrRow(rAddr), (PmemAddress)0x8765);

  pagePtr->setKVNodeAddrRow(rAddr, (ValuePtr *)0x9753);
  ASSERT_EQ(pagePtr->getKVNodeAddrRow(rAddr), (ValuePtr *)0x9753);

  std::string row01("r001", 4);
  ASSERT_EQ(valueSize, row01.size());
  std::string val01;
  ASSERT_EQ(pagePtr->setValueRow(rAddr, row01, valueSize), true);
  pagePtr->getValueRow(rAddr, valueSize, val01);
  ASSERT_EQ(val01, row01);

  std::string row02("r02", 4);
  ASSERT_EQ(valueSize, row02.size());
  std::string val02;
  ASSERT_EQ(pagePtr->setValueRow(rAddr, row02, valueSize), true);
  pagePtr->getValueRow(rAddr, valueSize, val02);
  ASSERT_EQ(val02, row02);

  std::string row03("row03", 5);
  rOff = pagePtr->getFirstZeroBit(maxRowCnt);
  ASSERT_EQ(rOff, 2);
  ASSERT_EQ(pagePtr->setValueRow(rAddr, row03, valueSize), false);
}

}  // namespace NKV
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
