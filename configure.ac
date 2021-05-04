#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.
#
# 1. run 'aclocal'
# 2. run 'autoheader'
# 3. run 'autoconf'
# 4. run './configure'  (optionally with parameters)
# 5. run 'make swi'
#
# subsequently: 'autoreconf -vfi'

AC_PREREQ([2.69])
AC_INIT(plkafka, 1.0.0)

AC_CONFIG_AUX_DIR([config])
#AC_CONFIG_SRCDIR([test/test.cpp])
AC_CONFIG_HEADERS([src/config.h])

# preset definitions
if test -z "$CFLAGS"; then
  CFLAGS="-g -O2 -fPIC -Wall"
fi

ARCH=$(uname -s)

# Checks for programs.
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_RANLIB

# Checks for libraries.
AC_SUBST(ARCH)
AC_SUBST(SWI_CFLAGS)
AC_SUBST(SWI_LIBS)
AC_SUBST(SWI_BASE)
AC_SUBST(SWI_ARCH)
AC_SUBST(SWI_SOLIBDIR)
AC_SUBST(SWI_PLLIBDIR)
AC_SUBST(RDKAFKA_CFLAGS)
AC_SUBST(RDKAFKA_LIBS)
AC_SUBST(LIBCRYPTO_CFLAGS)
AC_SUBST(LIBCRYPTO_LIBS)
AC_SUBST(LIBSSL_CFLAGS)
AC_SUBST(LIBSSL_LIBS)
AC_SUBST(GI_CFLAGS)
AC_SUBST(GI_LIBS)
AC_SUBST(GI_DATADIR)
AC_SUBST(SOTARGET)
AC_SUBST(PLTARGET)

if [[ $ARCH = "Linux" ]]; then
PKG_PROG_PKG_CONFIG()
PKG_CHECK_MODULES([SWI], [swipl libcrypto libssl zlib])

SWI_BASE=$($PKG_CONFIG --variable=PLBASE swipl)
SWI_ARCH=$($PKG_CONFIG --variable=PLARCH swipl)
SWI_SOLIBDIR=$SWI_BASE/lib/$SWI_ARCH
SWI_PLLIBDIR=$SWI_BASE/library
fi

AC_CHECK_FILE(./librdkafka-1.6.1/BUILD/lib/librdkafka.a,
              [RDKAFKA_CFLAGS="-I./librdkafka-1.6.1/BUILD/include"],[])
AC_CHECK_FILE(./librdkafka-1.6.1/BUILD/lib/librdkafka.a,
              [RDKAFKA_LIBS="./librdkafka-1.6.1/BUILD/lib/librdkafka.a"],
              [PKG_CHECK_MODULES([RDKAFKA], [rdkafka])] )


if [[ $ARCH = "Darwin" ]]; then
  SWI_BASE=/usr/local/opt/swi-prolog
  if test -d "${SWI_BASE}"; then
    MACHINE=$(uname -m)
    SWI_CFLAGS="-I${SWI_BASE}/libexec/lib/swipl/include"
    SWI_LIBS="-L${SWI_BASE}/libexec/lib/swipl/lib/${MACHINE}-darwin -lswipl"
  else
    AC_MSG_FAILURE([cannot find SWI Prolog])
  fi
  LIBCRYPTO_BASE=/usr/local/opt/openssl
  if test -d "${LIBCRYPTO_BASE}"; then
    MACHINE=$(uname -m)
    LIBCRYPTO_CFLAGS="-I${LIBCRYPTO_BASE}/include"
    LIBCRYPTO_LIBS="-L${LIBCRYPTO_BASE}/lib -lcrypto"
  else
    AC_MSG_FAILURE([cannot find OpenSSL])
  fi
  RDKAFKA_BASE=/usr/local/opt/librdkafka
  if test -d "${RDKAFKA_BASE}"; then
    MACHINE=$(uname -m)
    RDKAFKA_CFLAGS="-I${RDKAFKA_BASE}/include"
    RDKAFKA_LIBS="-L${RDKAFKA_BASE}/lib -lrdkafka"
  else
    AC_MSG_FAILURE([cannot find library rdkafka])
  fi
fi

# Checks for header files.
AC_CHECK_HEADERS([stdint.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_CHECK_SIZEOF(long)

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([strdup])

AC_CONFIG_FILES([Makefile])

AC_OUTPUT

