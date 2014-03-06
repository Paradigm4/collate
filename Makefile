
ifeq ($(SCIDB),) 
  X := $(shell which scidb 2>/dev/null)
  ifneq ($(X),)
    X := $(shell dirname ${X})
    SCIDB := $(shell dirname ${X})
  endif
endif

CFLAGS=-pedantic -W -Wextra -Wall -Wno-variadic-macros -Wno-strict-aliasing -Wno-long-long -Wno-unused-parameter -fPIC -D_STDC_FORMAT_MACROS -Wno-system-headers -isystem -O2 -g -DNDEBUG -ggdb3  -D_STDC_LIMIT_MACROS
INC=-I. -DPROJECT_ROOT="\"$(SCIDB)\"" -I"$(SCIDB)/3rdparty/boost/include/" -I"$(SCIDB)/include" -I./extern

LIBS=-shared -Wl,-soname,libcollate.so -ldl -L. -L"$(SCIDB)/3rdparty/boost/lib" -L"$(SCIDB)/lib" -Wl,-rpath,$(SCIDB)/lib:$(RPATH)

all:
	@if test ! -d "$(SCIDB)"; then echo  "Error. Try:\n\nmake SCIDB=<PATH TO SCIDB INSTALL PATH>"; exit 1; fi
	$(CXX) $(CFLAGS) $(INC) -o LogicalCollate.o -c LogicalCollate.cpp
	$(CXX) $(CFLAGS) $(INC) -o PhysicalCollate.o -c PhysicalCollate.cpp
	$(CXX) $(CFLAGS) $(INC) -o libcollate.so plugin.cpp LogicalCollate.o PhysicalCollate.o $(LIBS)
	@echo "Now copy libcollate.so to your SciDB lib/scidb/plugins directory and restart SciDB."

clean:
	rm -f *.so *.o
