//
//  pod_test.cc
//  PROJECT pod_test
//
//  Created by zhenliu on 23/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "schema.h"
#include "gtest/gtest.h"

struct TestStructure {
  std::string a;
  int b;
};

TEST(AddTest, TestAddResult)
{
  ASSERT_EQ(true, std::is_pod<NKV::ValueContent>::value);
  ASSERT_EQ(4, sizeof(NKV::ValueContent));
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
