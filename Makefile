RM=rm -f
CC=gcc
CXX=g++

CXXFLAGS=-Wall -Wextra -pedantic -std=c++14 \
         -I KeyStore -I Log -I Reader -I RingBuffer -I RunWriter -I RunCreator \
         -I SyncIO -I RunReader -I Writer -I libs/lz4/lib \
         -pthread

CFLAGS=-Wall -Wextra -pedantic -std=c11

SRCS=fort.cpp \
     Log/Log.cpp \
     KeyStore/KeyStore.cpp \
     SyncIO/SyncIO.cpp \
     Reader/Reader.cpp \
     Reader/TextReader.cpp \
     RingBuffer/RingBuffer.cpp \
     RunCreator/RunCreator.cpp \
     RunWriter/RunWriter.cpp \
     RunWriter/RawRunWriter.cpp \
     RunWriter/LZ4RunWriter.cpp \
     RunReader/RunReader.cpp \
     RunReader/RawRunReader.cpp \
     RunReader/LZ4RunReader.cpp \
     RunMerger/RunMerger.cpp \
     Writer/Writer.cpp \
     Writer/TextWriter.cpp \
     libs/lz4/lib/lz4.c \
     libs/lz4/lib/lz4hc.c \
     libs/lz4/lib/lz4frame.c \
     libs/lz4/lib/xxhash.c

CXX_SRCS=$(filter %.cpp,$(SRCS))
C_SRCS=$(filter %.c,$(SRCS))

CXX_OBJS=$(CXX_SRCS:.cpp=.o)
C_OBJS=$(C_SRCS:.c=.o)

all: CXXFLAGS += -O3 -flto
all: CFLAGS += -O3 -flto
all: fort

debug: CXXFLAGS += -DBUILD_DEBUG -g
debug: CFLAGS += -DBUILD_DEBUG -g
debug: fort

fort: $(C_OBJS) $(CXX_OBJS)
	$(CXX) $(CXXFLAGS) -o fort $(C_OBJS) $(CXX_OBJS)

depend: .depend

.depend: $(SRCS)
	$(RM) ./.depend
	$(CC) $(CFLAGS) -MM $(C_SRCS) >>./.depend;
	$(CXX) $(CXXFLAGS) -MM $(CXX_SRCS) >>./.depend;

clean:
	$(RM) $(C_OBJS) $(CXX_OBJS) ./.depend fort

include .depend

