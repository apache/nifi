#MAVEN_FLAGS="-Dmaven.test.skip=true"
MAVEN_FLAGS=""

cd misc/nar-maven-plugin && \
mvn $MAVEN_FLAGS install && \
cd ../../commons/nifi-parent && \
mvn $MAVEN_FLAGS install && \
cd ../../nifi-bootstrap && \
mvn $MAVEN_FLAGS install && \
cd ../nifi-api && \
mvn $MAVEN_FLAGS install && \
cd ../commons/ && \
cd	nifi-stream-utils && \
mvn $MAVEN_FLAGS install && \
cd	../wali && \
mvn $MAVEN_FLAGS install && \
cd	../flowfile-packager && \
mvn $MAVEN_FLAGS install && \
cd	../core-flowfile-attributes && \
mvn $MAVEN_FLAGS install && \
cd	../data-provenance-utils && \
mvn $MAVEN_FLAGS install && \
cd	../naive-search-ring-buffer && \
mvn $MAVEN_FLAGS install && \
cd	../nifi-expression-language && \
mvn $MAVEN_FLAGS install && \
cd	../nifi-file-utils && \
mvn $MAVEN_FLAGS install && \
cd	../nifi-logging-utils && \
mvn $MAVEN_FLAGS install && \
cd	../nifi-properties && \
mvn $MAVEN_FLAGS install && \
cd	../nifi-security-utils && \
mvn $MAVEN_FLAGS install && \
cd	../nifi-utils && \
mvn $MAVEN_FLAGS install && \
cd	../nifi-socket-utils && \
mvn $MAVEN_FLAGS install && \
cd	../nifi-web-utils && \
mvn $MAVEN_FLAGS install && \
cd	../processor-utilities && \
mvn $MAVEN_FLAGS install && \
cd	../remote-communications-utils && \
mvn $MAVEN_FLAGS install && \
cd	../search-utils && \
mvn $MAVEN_FLAGS install && \
cd ../../extensions/file-authorization-provider && \
mvn $MAVEN_FLAGS install && \
cd ../../nifi-mock && \
mvn $MAVEN_FLAGS install && \
cd ../nar-bundles/ && \
cd	nar-container-common && \
mvn $MAVEN_FLAGS install && \
cd	../jetty-bundle && \
mvn $MAVEN_FLAGS install && \
cd	../standard-services-api-bundle && \
mvn $MAVEN_FLAGS install && \
cd	../ssl-context-bundle && \
mvn $MAVEN_FLAGS install && \
cd	../distributed-cache-services-bundle && \
mvn $MAVEN_FLAGS install && \
cd	../standard-bundle && \
mvn $MAVEN_FLAGS install && \
cd	../hadoop-libraries-bundle && \
mvn $MAVEN_FLAGS install && \
cd	../hadoop-bundle && \
mvn $MAVEN_FLAGS install && \
cd	../volatile-provenance-repository-bundle && \
mvn $MAVEN_FLAGS install && \
cd	../persistent-provenance-repository-bundle && \
mvn $MAVEN_FLAGS install && \
cd	../framework-bundle && \
mvn $MAVEN_FLAGS install && \
cd	../execute-script-bundle && \
mvn $MAVEN_FLAGS install && \
cd	../monitor-threshold-bundle && \
mvn $MAVEN_FLAGS install && \
cd	../update-attribute-bundle && \
mvn $MAVEN_FLAGS install && \
cd ../../assemblies/nifi 
mvn assembly:assembly
