//
//  ../tests/new_operator_test.cc
//  PROJECT ../tests/new_operator_test
//
//  Created by zhenliu on 23/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include <iostream>
struct Test{
  int a;
};
int main(int argc, char *argv[])
{

  Test *p = static_cast<Test*>(operator new (10*sizeof(Test)));
  delete p;
  p = static_cast<Test*>(operator new (10*sizeof(Test)));
  for(auto i  = 0; i< 10;i++)
    delete (p+i*sizeof(Test));
  return 0;
}
