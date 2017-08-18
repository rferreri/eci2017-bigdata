This folder contains a file (HBaseTest.java) that implements a simple application that uses HBase operations.

This application reads data from the directory 'data'. This data must follow a strict CSV format. In the case of this example, the data represents a list of all the tourists arriving to Brazil divided by country, way in (air, sea, land or river), year and month. There is a file (touristData.csv) inside this folder that includes data from January, 1989 to December, 2015.

The application creates a table (named 'tourists') with two column families: 'origin' (that will contain data relating the origin of the count in this row, for example, the country and continent of this row of tourists) and 'entry' (that will contain data regarding the entry of the count in this row, for example, the state, the way in, year, month and total). After doing that, it populates this table using the data from the input file.

In order to compile the application, use the following line:

	javac -cp $(hbase classpath):$(hadoop classpath) HBaseTest.java 

And to execute it, use:

	java -cp $(hbase classpath):$(hadoop classpath):. HBaseTest