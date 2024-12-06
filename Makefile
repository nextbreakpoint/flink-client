.PHONY: build
build:
	mvn package

.PHONY: clean
clean:
	mvn clean

.PHONY: test
test:
	mvn test

.PHONY: verify
verify:
	mvn verify

.PHONY: package
package:
	mvn package -DskipTests=true

.PHONY: install
install:
	mvn install -DskipTests=true

.PHONY: start
start:
	mvn io.fabric8:docker-maven-plugin:start@start-flink

.PHONY: stop
stop:
	mvn io.fabric8:docker-maven-plugin:stop@stop-flink

.PHONY: release
release:
	mvn -Dchannel=ossrh clean deploy
	mvn -Dchannel=github de.jutzig:github-release-plugin:release

.PHONY: version
version:
	@echo $(shell mvn -q help:evaluate -Dexpression=project.version -DforceStdout)
