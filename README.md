What is SenseiBA
===============
([https://github.com/sensei-ba/sensei/wiki/Introduction](https://github.com/sensei-ba/sensei/wiki/Introduction))

SenseiBA is a distributed, elastic OLAP datastore campable of storing hundred billions documents and providing the real time seach.

------------------------------------

### Wiki

Wiki is available at: 

[http://linkedin.jira.com/wiki/display/SENSEI/Home](http://linkedin.jira.com/wiki/display/SENSEI/Home)

### Issues

Issues are tracked at: 

[http://linkedin.jira.com/browse/SENSEI](http://linkedin.jira.com/browse/SENSEI)

### Mailing List / Discussion Group

[http://groups.google.com/group/sensei-search](http://groups.google.com/group/sensei-search)

### Getting Started

1. Build

        ./bin/build.sh

2. Start ZooKeeper

        ./bin/zookeeper-server-start.sh resources/zookeeper.properties

3. Run a search node(s)

        bin/start-sensei-node.sh example/cars/conf

5. Starting command-line client app

        bin/sensei-client.sh host port (default: localhost 8080)

And/Or:

Go to web console: [http://localhost:8080](http://localhost:8080)
