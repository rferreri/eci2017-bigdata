This folder contains three .java files that implement three distributed co-ordination protocols (barriers, producer-consumer and read-write locks).
Besides the code that implements this protocols, there is also code that uses Java threads in order to test the functionalities.
The location of ZooKeeper's server (host and port), alongside other parameters used, is set in each .java file separately.

To compile all the files at once, please use:

	javac -cp $ZOOKEEPER_CLASSPATH *.java

where $ZOOKEEPER_CLASSPATH should be previously defined and include all ZooKeeper's jars and dependencies.

To run, use:

	java -cp $ZOOKEEPER_CLASSPATH:. <MainClass>

where <MainClass> is one of 'ZooKeeperBarrier', 'ZooKeeperProducerConsumer' and 'ZooKeeperReadWriteLocks'.


+ ZooKeeperBarrier.java: 

This file includes the implementation of a simple barrier using ZooKeeper primitives. A variable number of threads execute some arbitrary work (represented by a random wait time) and then call the function 'barrier' in order to wait for the rest of the threads. The barrier is lifted after all the threads have finished their work and joined it.
The barrier is implemented using two ZooKeeper znodes: one of them (/barrier/children) contains one child for each thread that has put itself in the barrier, while the other node (/barrier/lifted) signals the rest of the threads that the barrier has been lifted. This last node is set by the first thread that detects that the number of children of the node /barrier/children is equal to the number of threads needed to lift the barrier.


+ ZooKeeperProducerConsumer.java:

This file includes the implementation of a producer-consumer scheme using ZooKeeper primitives. There are two classes, 'Producer' and 'Consumer'. The producers produce data (in this case, a random number) and then put it into a queue that is defined as a znode (/queue). On the other hand, the consumers consume the data produced by the producers, which means taking the first element in the queue. The position of the first element is calculated using the sequential number that is added to the name of the path when using the SEQUENTIAL property.
In the example, some producers and consumers are created and put to work using random waiting times to simulate a real life environment where each of them might be doing heavy work.


+ ZooKeeperReadWriteLocks.java: 

This file includes the implementation of read-write locks. As its name suggests, this type of locks allow multiple readers at the same time, but only one writer. The implementation defines the class ReadWriteLock that includes the methods getReadLock, getWriteLock and returnLock. The locks are implemented using a queue (/locks/lock) that is stored as a ZooKeeper node. A reader or writer that wishes to acquire the lock puts himself at the end of this queue and then tries to determine whether he is the first in the queue (if he is a writer) or if he has only readers ahead of him (if he is a reader). In this scheme, starvation would never happen because the order of the queue if strictly respected to decide whether the reader or writer may acquire the lock.
In the example, there are two classes defined (Reader and Writer) that are put to work using random waiting times to simulate a real life environment where each of them might be doing heavy work.


The code also includes additional comments that are helpful to understand the way in which things are implemented.