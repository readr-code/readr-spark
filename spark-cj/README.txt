// note: to compile the cj parser on some platforms, one needs to add #include <getopt.h>
// to second-stage/features/best-parses.cc
// or to second-stage/programs/features/best-parses.cc

// if you get the following error
evalb.c:23:20: error: malloc.h: No such file or directory
then just comment out #include <malloc.h> in eval/evalb.c

// If you compile this on Mac, make sure you use the gcc compiler, not clang which is default
// with macports you can switch
//   sudo port install llvm-gcc42
//   sudo port install gcc_select
//   sudo port select --list gcc
//   sudo port select gcc llvm-gcc42
//   readlink `which c++`


// test, using:
//cat steedman.txt | first-stage/PARSE/parseIt -l399 -N50 -K first-stage/DATA/EN/ | second-stage/programs/features/best-parses -l second-stage/models/ec50spfinal/features.gz second-stage/models/ec50spfinal/cvlm-l1c10P1-weights.gz
