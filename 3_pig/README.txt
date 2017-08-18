This folder contains a file (script.pig) that implements a Pig query using simple database operators.

This application reads data from the directory 'input'. This data must follow a strict CSV format. In the case of this example, the data represents a list of all the tourists arriving to Brazil divided by country, way in (air, sea, land or river), year and month. There is a file (touristData.csv) inside this folder that includes data from January, 1989 to December, 2015.

The application defines the schema of the data being read, and afterwards uses Pig operators to obtain an ordered list of the total number of tourists by country. Finally, it stores the data in the 'output' folder.

In order to run the Pig script, use the following line:

	pig -x local script.pig