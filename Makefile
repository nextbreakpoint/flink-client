.PHONY: package
package:
	mvn clean package

.PHONY: install
install:
	mvn clean install

.PHONY: test
test:
	mvn clean test

.PHONY: verify
verify:
	mvn clean verify

.PHONY: start
start:
	mvn -pl core io.fabric8:docker-maven-plugin:start@start-flink

.PHONY: stop
stop:
	mvn -pl core io.fabric8:docker-maven-plugin:stop@stop-flink

.PHONY: release
release:
	mvn -Dchannel=ossrh clean deploy
	mvn -Dchannel=github de.jutzig:github-release-plugin:release
