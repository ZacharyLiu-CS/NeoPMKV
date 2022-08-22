//
//  add_test.cc
//  PROJECT add_test
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//
#include "gtest/gtest.h"
#include "add.h"

TEST(AddTest, TestAddResult){
  ASSERT_EQ(3, add(1,2));
  ASSERT_EQ(4, add(2,2));
  ASSERT_EQ(5, add(2,3));
  ASSERT_EQ(6, add(4,2));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

