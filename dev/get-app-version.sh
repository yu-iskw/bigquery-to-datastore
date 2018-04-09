#
# Copyright (c) 2017 Yu Ishikawa.
#

#
# This script is used for extracting the application version from pom.xml.
#
PROJECT_DIR=$(dirname $(dirname $(readlink -f $0)))
XML_TAG=$(grep -e '<version>[0-9\.]*</version>' "${PROJECT_DIR}/pom.xml")
VERSION=$(echo "$XML_TAG" | sed -e 's/<version>//' -e 's/<\/version>//' | tr -d '[:space:]')
echo "$VERSION"
