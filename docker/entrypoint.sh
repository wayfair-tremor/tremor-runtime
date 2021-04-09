#!/bin/sh
set -x

# Possible environment variables:
# TREMOR_DIR: The base directory for tremor config (`/etc/tremor`)
# CFG_DIR: Directory to load config from (`${TREMOR_DIR}/config`)
# LOGGER_FILE: the logger configuration (`${TREMOR_DIR}/logger.yaml`)
# SLEEP: Number of seconds to sleep before starting tremor

if [ -n "${SLEEP+x}" ]
then
    sleep "$SLEEP"
fi

if [ -z ${TREMOR_DIR+x} ]
then
    TREMOR_DIR="/etc/tremor"
fi

if [ -z ${CFG_DIR+x} ]
then
    CFG_DIR="${TREMOR_DIR}/config"
fi

if [ -z ${LOGGER_FILE+x} ]
then
    LOGGER_FILE="${TREMOR_DIR}/logger.yaml"
fi

if [ "$#" != "0" ]
then
    ARGS=$*
else

    ARTEFACTS=""

    # Load *.yaml files
    YAMLS=$(find ${CFG_DIR} -name '*.yaml' -print 2>/dev/null | wc -l)
    if [ "$YAMLS" != 0 ]
    then
        ARTEFACTS="$ARTEFACTS ${CFG_DIR}/*.yaml"
    fi

    # Load *.yml files
    YMLS=$(find ${CFG_DIR} -name '*.yml' -print 2>/dev/null | wc -l)
    if [ "$YMLS" != 0 ]
    then
        ARTEFACTS="$ARTEFACTS ${CFG_DIR}/*.yml"
    fi

    # Load *.trickle files
    QUERIES=$(find ${CFG_DIR}/ -name '*.trickle' -print 2>/dev/null | wc -l)
    if [ "$QUERIES" != 0 ]
    then
        ARTEFACTS="$ARTEFACTS ${CFG_DIR}/*.trickle"
    fi
    ARGS="server run --logger-config ${LOGGER_FILE}"

    if [ ! -z "${ARTEFACTS}" ]
    then
        ARGS="${ARGS} -f ${ARTEFACTS}"
    fi
fi

# IMPORTANT: do not quote ARGS, no matter what shellcheck tells you
exec /usr/bin/tini /tremor -- ${ARGS}