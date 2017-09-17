.PHONY: test checkstyle clean

all: checkstyle test compile

compile:
	  mvn clean compile

test:
	  mvn test

checkstyle:
	  mvn checkstyle:checkstyle

clean:
	  mvn clean
