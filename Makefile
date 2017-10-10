#
# Copyright (c) 2017 Yu Ishikawa.
#

.PHONY: test checkstyle clean

all: checkstyle test package

package:
	  mvn clean package

test:
	  mvn test

checkstyle:
	  mvn checkstyle:checkstyle

clean:
	  mvn clean
