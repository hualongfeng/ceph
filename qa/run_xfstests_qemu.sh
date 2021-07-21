#!/usr/bin/env bash
#
# TODO switch to run_xfstests.sh (see run_xfstests_krbd.sh)

set -x

[ -n "${TESTDIR}" ] || export TESTDIR="/tmp/cephtest"
[ -d "${TESTDIR}" ] || mkdir "${TESTDIR}"

URL_BASE="http://10.239.40.205/software"
SCRIPT="run_xfstests-obsolete.sh"

cd "${TESTDIR}"

wget -O "${SCRIPT}" "${URL_BASE}/${SCRIPT}"
chmod +x "${SCRIPT}"

# tests excluded fail in the current testing vm regardless of whether
# rbd is used

#for testid in {1..7} {9..17} {19..26} {28..49} {51..61} 63 {66..67} {69..72} 73 74 {75..79} 83 {85..105} {108..110} {112..135} \
#    {137..170} {174..191} {193..204} {206..217} {220..227} {230..231} 233 {235..241} {243..249} \
#    {251..262} {264..278} {281..286} {288..289}
for testid in {1..4}
do
    ./"${SCRIPT}" -c 1 -f xfs -t /dev/vdb -s /dev/vdc ${testid}
done
#./"${SCRIPT}" -c 1 -f xfs -t /dev/vdb -s /dev/vdc \
#./"${SCRIPT}" -c 1 -f xfs -t /dev/vdb -s /dev/vdc \
#    1-7 9-17 19-26 28-49 51-61 63 66-67 69-72 75-79 83 85-105 108-110 112-135 \
#    137-170 174-191 193-204 206-217 220-227 230-231 233 235-241 243-249 \
#    251-262 264-278 281-286 288-289
STATUS=$?

rm -f "${SCRIPT}"

exit "${STATUS}"
