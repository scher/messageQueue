# Message Queues
Local implementation of AWS SQS

# Implementation details

Project represents implementation of AWS SQS in two flavors:

 1. An in-memory implementation. This is suitable for single JVM usage. Thread safe implementation.

 2. Persistence file system implementation. This is suitable for single host usage.
    This implementation save it's state between application restarts. Thread safe and inter-process safe implementation.

 3. AWS SQS adapter.

# Installation and start

Set environment variable `falvor` to `memory|local|prod`
If decided to use `prod` configuration then you should configure your AWS credentials. [See how to do its](https://github.com/aws/aws-sdk-java/tree/master/src/samples/AmazonSimpleQueueService) 

```bash 
    mvn clean package
    export flavor=memory
    java -jar target/queue-service-1.0.0.jar
```




 
