#ifndef GRID_UTIL_H
#define GRID_UTIL_H

#include <ctime>
#include <string>

namespace Grid
{

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
