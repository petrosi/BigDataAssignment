BASEDIR=$(dirname $0)
mvn package shade:shade -f "${BASEDIR}/../pom.xml"