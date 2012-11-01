1. To run the zookeeper managed instance:
   a. go to Sensei root and run bin/start-sensei-node.sh sensei-ba/config-example/target/config/
   b. cd to sensei-ba/ba-core and run the mvn  exec:java  -Dexec.mainClass=com.senseidb.ba.util.BaClient -Dexec.args="ba-server"
   c. add segments through the console
   
2. to run the standalone instance:
   a. go to Sensei root and run bin/start-sensei-node.sh sensei-ba/config-example/target/config-standalone/
   b. copy avro files to the /tmp/ba-index-standalone
   