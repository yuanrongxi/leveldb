#ifndef __PORT_LEVEL_DB_H_
#define __PORT_LEVEL_DB_H_

#include <string.h>

#if defined(LEVELDB_PLATFORM_POSIX)
#  include "port_posix.h"
#elif defined(LEVELDB_PLATFORM_CHROMIUM)
#  include "port_chromium.h"
#endif


#endif
