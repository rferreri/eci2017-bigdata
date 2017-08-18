This folder contains a file (TouristsByCountry.py) that implements a simple application using Spark.

This application reads data from the directory 'input'. This data must follow a strict CSV format. In the case of this example, the data represents a list of all the tourists arriving to Brazil divided by country, way in (air, sea, land or river), year and month. There is a file (touristData.csv) inside this folder that includes data from January, 1989 to December, 2015.

After reading the data, the application makes use of Spark transformations in order to obtain the list of countries with most tourists in Brazil, and stores this information in the 'output' folder.

The application was programmed using Python's Spark API, and must be submited to Spark using the following command:

	spark-submit TouristsByCountry.py