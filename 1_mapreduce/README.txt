This folder contains a file (TouristsByCountry.java) that implements a simple data analytics application using the MapReduce programming model (Apache Hadoop).

This application reads data from the directory 'input'. This data must follow a strict CSV format. In the case of this example, the data represents a list of all the tourists arriving to Brazil divided by country, way in (air, sea, land or river), year and month. There is a file (touristData.csv) inside this folder that includes data from January, 1989 to December, 2015.

The application defines a map operation over the data, that split the input lines into fields and outputs the country as the key and the string "<wayin> <count>" as the value. After that, the reduce operation reads the grouped keys and outputs one line per country that includes the totals per way in (Air, Sea, Land and River). The final data is sorted in lexicographical order.

In order to compile the Hadoop application, use the following lines:
	hadoop com.sun.tools.javac.Main TouristsByCountry.java
	jar cf TouristsByCountry.jar TouristsByCountry*.class

Finally, to run the application, use the following line:
	hadoop jar TouristsByCountry.jar TouristsByCountry ./input ./output