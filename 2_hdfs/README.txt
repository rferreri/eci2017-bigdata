This folder contains a file (HDFSTest.java) that implements a simple application that uses basic HDFS file-system operations.

This application connects to the HDFS defined in the variable hdfsPath, creates a directory, copies the HDFSTest.java file from the local file system into the HDFS, creates a new file in the HDFS and appends some data into it, and finally move the new file into the local file system.

In order to compile the Hadoop application, use the following lines:
	hadoop com.sun.tools.javac.Main HDFSTest.java
	jar cf HDFSTest.jar HDFSTest*.class

Finally, to run the application, use the following line:
	hadoop jar HDFSTest.jar HDFSTest