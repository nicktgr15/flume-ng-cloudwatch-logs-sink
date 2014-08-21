flume-ng-cloudwatch-logs-sink
=============================

Installation
-----------

1. ```mnv clean package```
2. copy generated ```flume-ng-cloudwatch-logs-sink``` jar under flume lib directory
3. copy ```aws java sdk``` provided dependency jar under flume lib directory (```wget http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.8.9.1/aws-java-sdk-1.8.9.1.jar```)

Example Config
--------------

```
a1.sinks.k1.type = com.nicktgr15.CloudwatchLogsSink
a1.sinks.k1.logGroupName = <log group name>
a1.sinks.k1.logStreamName = <log stream name>
a1.sinks.k1.awsAccessKeyId = <your aws access key>
a1.sinks.k1.awsSecretAccessKey = <your aws secret key>
```
