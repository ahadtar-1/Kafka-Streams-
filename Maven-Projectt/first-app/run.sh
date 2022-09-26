sudo mvn clean compile package
sudo mvn clean compile assembly::single
sudo chmod +x target/first-app-1.0-SNAPSHOT-jar-with-dependencies.jar
sudo java -jar target/first-app-1.0-SNAPSHOT-jar-with-dependencies.jar
