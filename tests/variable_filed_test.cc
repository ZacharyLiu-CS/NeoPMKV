#include <iostream>

#include <cstdlib>
#include <memory>
#include <random>
#include "gtest/gtest.h"
#include "schema.h"

namespace NKV {
TEST(VariableFieldTest, TestSize) {
  EXPECT_EQ(sizeof(VarStrField), FTSize[(uint8_t)FieldType::VARSTR]);
}

}  // namespace NKV
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
