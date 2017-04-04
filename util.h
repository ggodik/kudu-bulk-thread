#ifndef GRID_UTIL_H
#define GRID_UTIL_H

#include <ctime>
#include <string>

namespace Util
{
  // provided here as a quick implementation
  // become some version of this code has to compile
  // on rhel6 and gcc 4.8 without C++11 support
  std::string to_string(int i);

  std::string GetString(int size = 26);
  
  class Timer {
  public:
    Timer(const std::string &);
    ~Timer();
  private:
    std::clock_t start;
    const std::string msg;
  };

}

#endif
