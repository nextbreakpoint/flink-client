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
	docker compose up --wait

.PHONY: stop
stop:
	docker compose down

.PHONY: terminate
terminate:
	docker compose down --volumes

.PHONY: release
release:
	mvn -Dchannel=ossrh clean deploy
	mvn -Dchannel=github de.jutzig:github-release-plugin:release

.PHONY: version
version:
	@echo $(shell mvn -q help:evaluate -Dexpression=project.version -DforceStdout)
