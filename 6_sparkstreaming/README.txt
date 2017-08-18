This folder contains a file (SpotifySongs.py) that implements a simple streaming application using Spark Streaming.

This application reads data from the directory 'input'. Each time it detects a new file in the folder, it reads the new data. This data must follow a strict CSV format. In the case of this example, the data represents a list of all the songs being streamed right now by users worldwide in an Spotify-like platform. There is a file in 'input/song' that shows an example of the correct format of this file.

After reading the data, the application makes use of Spark transformations in order to obtain a list of the most streamed songs in this moment (according to the current stream). This 10 most streamed songs are then printed into the console.

Because of Spark streaming, each time a new batch of data is received in the stream, the application reads this new data, recalculates the most streamed songs
and prints them to the console again.

The application was programmed using Python's Spark API, and must be submited to Spark using the following command:

	spark-submit --master local[2] SpotifySongs.py