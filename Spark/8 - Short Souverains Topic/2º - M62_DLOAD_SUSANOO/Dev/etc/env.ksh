#!/bin/ksh -x
#========================================================================
#
# Variables d'environement GLOBALES a Reporting SUSANOO
#
#========================================================================


#!/bin/ksh -x
#========================================================================
#
# Variables d'environnement du groupe ?M62_DSYNC_DSUSANOO_TT
#
#========================================================================
# Variables communes
_TIMESTAMP=$(date +"%d-%m-%Y_%H%M%S")
export _LOG_FILE="${M62LOGDIR}/$(basename ${0})_${_TIMESTAMP}_$$.log"