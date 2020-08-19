#!/bin/sh
pyclean () {
    find . -type f -name "*.py[co]" -delete
    find . -type d -name "__pycache__" -delete
    find . -type f -name "*.log" -delete
    find . -type f -name ".DS_Store" -delete
}

normalDir="`cd "${dirToNormalize}";pwd`"

SCRIPTS_DIR=$(cd `dirname $BASH_SOURCE`; pwd)
BUILD_DIR="${SCRIPTS_DIR}/../build"
SOURCE_DIR="${SCRIPTS_DIR}/../src"

echo "DEBUG: SCRIPTS_DIR: ${SCRIPTS_DIR}"
echo "DEBUG:   BUILD_DIR: ${BUILD_DIR}"
echo "DEBUG:  SOURCE_DIR: ${SOURCE_DIR}"

echo "Cleaning up build directory: ${BUILD_DIR}"
rm -rf "${BUILD_DIR}"
mkdir -p ${BUILD_DIR}/{lambda,cloudformation}

echo "Building app-request-reader"
cd ${SOURCE_DIR}/lambda/app-request-reader
pyclean
zip "${BUILD_DIR}/lambda/app-request-reader.zip" *.py >/dev/null 2>&1

echo "Building gap-watch-request-reader"
cd ${SOURCE_DIR}/lambda/gap-watch-request-reader
pyclean
zip "${BUILD_DIR}/lambda/gap-watch-request-reader.zip" *.py >/dev/null 2>&1

echo "Building batch-request-reader"
cd ${SOURCE_DIR}/lambda/batch-request-reader
pyclean
# include the py and pem files
zip ${BUILD_DIR}/lambda/batch-request-reader.zip *.p* >/dev/null 2>&1

echo "Copying migrator-app"
cd ${SOURCE_DIR}/migrator-app
pyclean
cp -RL . ${BUILD_DIR}/migrator-app 

echo "Copying configure app"
cd ${SOURCE_DIR}/configure
pyclean
cp -RL . ${BUILD_DIR}/configure

echo "Copying CloudFormation templates"
cd ${SOURCE_DIR}/cloudformation
cp *.yaml ${BUILD_DIR}/cloudformation/

echo "Copying lambda packs"
cd "${SCRIPTS_DIR}/../lib/lambda"
cp *.zip ${BUILD_DIR}/lambda/

echo "Creating a package: cosmosdb-migrator.tgz"
cd ${BUILD_DIR}
tar -czf cosmosdb-migrator.tgz * >/dev/null 2>&1
rm -rf migrator-app/ cloudformation/ configure/ lambda/