#include "util.h"
#include <cstdlib>
#include <vector>
#include <iostream>
#include <cstdio>

namespace Util
{
  using namespace std;
  
  std::string ALPHABET("ABCDEFGHIJKLMNOPQRSTUVWXYZ");

  std::string GetString(int size)
  {
    std::swap(ALPHABET[rand() % size],
	      ALPHABET[rand() % size]);;
    if(size == ALPHABET.size())
      return ALPHABET;
    else
      return ALPHABET.substr(size);
  }

  Timer::Timer(const string& _msg)
    : msg(_msg),
      start(std::chrono::high_resolution_clock::now())
  {}

  Timer::~Timer()
  {
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> dur = end - start;
    cout << msg
	 << " took:"
	 << dur.count()
	 << "s"
	 << endl;
  }
  
  std::string to_string(int i)
  {
    char buf[50];
    sprintf(buf, "%d", i);
    return string(buf);
  }


}
