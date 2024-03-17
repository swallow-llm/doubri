CC		= g++
CXXFLAGS= -O3 -Icontrib
CPPFLAGS= -MMD
TARGET	= dabri-minhash
SRCS	= minhash.cc MurmurHash3.cc
OBJS	= $(SRCS:%.cc=%.o)
DEPFILES= $(OBJS:%.o=%.d)
LIBDIR	=
LIBS    = 

all: dabri-minhash dabri-self dabri-other dabri-init dabri-apply

dabri-minhash:	minhash.o MurmurHash3.o
	$(CC) -o $@ $^ $(LIBDIR) $(LIBS)

dabri-self: dedup_self.o
	$(CC) -o $@ $^ $(LIBDIR) $(LIBS)

dabri-other: dedup_other.o
	$(CC) -o $@ $^ $(LIBDIR) $(LIBS)

dabri-init: flag_init.o
	$(CC) -o $@ $^ $(LIBDIR) $(LIBS)

dabri-apply: flag_apply.o
	$(CC) -o $@ $^ $(LIBDIR) $(LIBS)

clean:
	rm -f $(TARGET) $(OBJS) $(DEPFILES)

-include $(DEPFILES)
