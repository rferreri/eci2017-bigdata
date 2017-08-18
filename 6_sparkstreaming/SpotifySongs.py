from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    STREAMING_DIR = "input"

    spark = SparkContext(appName="SpotifyHotSongsRightNow")
    streaming = StreamingContext(spark, 1)

    # user_id, song_id, artist_id, song_title, artist_name, platform
    songs_streamed = streaming.textFileStream(STREAMING_DIR)

    hot_songs = songs_streamed.map(lambda line: line.split(","))\
                              .map(lambda line: ((line[3], line[4]), 1))\
                              .reduceByKey(lambda x, y: x + y)\
                              .map(lambda (x, y): (y, x))\
                              .transform(lambda x: x.sortByKey(False))\
                              .map(lambda (x, y): (y, x))\
                              .map(lambda ((song, artist), count): "%s, %s: %d streaming right now!" % (song, artist, count))

    hot_songs.pprint()

    streaming.start()
    streaming.awaitTermination()
