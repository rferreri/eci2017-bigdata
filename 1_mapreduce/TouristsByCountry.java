import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TouristsByCountry {
	public static class MapperClass extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// continent, country, state, wayin, year, month, count
			String [] splitted = value.toString().split(",");

			String wayIn = splitted[3];
			String countString = splitted[6];
			Integer count = 0;

			try {
				count = Integer.parseInt(countString);
			} catch (Exception e) {
				// Let count be zero and do nothing
			}

			Text country = new Text(splitted[1]);
			Text result = new Text();

			result.set(wayIn + " " + count);
			context.write(country, result);
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Integer totalByLand = 0;
			Integer totalBySea = 0;
			Integer totalByAir = 0;
			Integer totalByRiver = 0;

			for (Text value : values) {
				String [] sp = value.toString().split(" ");
				String wayIn = sp[0];
				Integer total = Integer.parseInt(sp[1]);
				if (wayIn.equals("\"Land\"")) {
					totalByLand += total;
				} else if (wayIn.equals("\"Sea\"")) {
					totalBySea += total;
				} else if (wayIn.equals("\"Air\"")) {
					totalByAir += total;
				} else if (wayIn.equals("\"River\"")) {
					totalByRiver += total;
				}
			}

			Text result = new Text();

			result.set(totalByLand + " " + totalBySea + " " + totalByAir + " " + totalByRiver);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Tourists by country");
		job.setJarByClass(TouristsByCountry.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
