KAFKA_JARS?=$(KAFKA_PATH)/libs

WORK_DIR:=$(strip $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST)))))
TARGET_HOME=$(WORK_DIR)/target
CLASS_HOME=$(TARGET_HOME)/classes
SRC_HOME=$(WORK_DIR)/src/main/java
KAFKA_VERSION?=2.4.0

CLASSES=JavaTestRunner.class

all: $(CLASSES)

JavaTestRunner.class: $(SRC_HOME)/JavaTestRunner.java
	mvn package -Dkafka.version=$(KAFKA_VERSION)

clean:
	rm -f $(CLASS_HOME)/*.class
	rm -f $(TARGET_HOME)/*.jar
