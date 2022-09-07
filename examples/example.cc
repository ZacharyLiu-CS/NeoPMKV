//
//  example.cc
//  PROJECT example
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "neopmkv.h"

#define LOG(msg)                   \
  do {                             \
    std::cout << msg << std::endl; \
  } while (0)

int main(){


  // NKV::NeoPMKV * neopmkv = new NKV::NeoPMKV();
  // NKV::Key key1 = 1;
  // std::string value1 = "1dsadadasdad1";
  // NKV::Key key2 = 2;
  // std::string value2 = "22-09kjlksjdlajlsd";
  // neopmkv->put(key1, value1);
  // neopmkv->put(key2, value2);

  // std::string read_value = "";
  // read_value.clear();
  // neopmkv->get(key1, read_value);
  // LOG("value:" << read_value);
  // read_value.clear();
  // neopmkv->get(key2, read_value);
  // LOG("value:" << read_value);
  
  // delete neopmkv;
  return 0;
}