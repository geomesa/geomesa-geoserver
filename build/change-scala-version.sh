binary_version=$1
if [[ $binary_version = "2.11" ]]; then
  full_version="2.11.7"
elif [[ $binary_version = "2.12" ]]; then
  full_version="2.12.13"
else
  echo "Scala version must be 2.11 or 2.12 - invalid scala version: $binary_version"
  exit 1
fi

sed -i -e "s#<scala.version>.*</scala.version>#<scala.version>${full_version}</scala.version>#" \
  -e "s#<scala.binary.version>.*</scala.binary.version>#<scala.binary.version>${binary_version}</scala.binary.version>#" pom.xml

for file in $(find . -name pom.xml); do
  sed -i -e "s#<artifactId>\(geomesa-gs-[a-z-]*\).*</artifactId>#<artifactId>\1_${binary_version}</artifactId>#" \
    -e "s#<artifactId>\(geomesa-geoserver\).*</artifactId>#<artifactId>\1_${binary_version}</artifactId>#" $file
done
