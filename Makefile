#
# Copyright (c) 2017 Yu Ishikawa.
#

.PHONY: test checkstyle clean

all: checkstyle test package

package:
		mvn clean package

test:
		mvn test
		bash ./dev/test.sh

checkstyle:
		mvn checkstyle:checkstyle

clean:
		mvn clean
