import socket

env = Environment(CXXFLAGS = '-O2 -ggdb3 -std=c++11')
env.Program('kudu-bulk-load',
	    ['bulk-load.cpp', 'util.cpp'],
            LIBS=['libkudu_client','pthread'])

