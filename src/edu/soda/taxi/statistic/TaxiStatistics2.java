package edu.soda.taxi.statistic;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaxiStatistics2 {

	// key = Id value = {time, free, pick up, take off, latitude, longitude,
	// dir, speed}

	public static class TSMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			String[] secondSplit = split[1].split(",");
			context.write(new Text(secondSplit[4] + "," + secondSplit[5] + "," + secondSplit[0]), new Text(split[0]
					+ "," + secondSplit[1] + "," + secondSplit[2] + "," + secondSplit[3] + "," + secondSplit[6] + ","
					+ secondSplit[7]));
		}
	}

	// key = {latitude, longitude, time} value = {ID, free, pick up, take off,
	// dir, speed}

	public static class TSReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<String> IDSet = new HashSet<>();
			long count = 0;
			long freeCount = 0;
			long pickUpCount = 0;
			long takeOffCount = 0;
			float speed = 0;
			int[] dir = new int[9];
			Arrays.fill(dir, 0);
			for (Text val : values) {
				count++;
				String[] record = val.toString().split(",");
				IDSet.add(record[0]);
				freeCount += Integer.parseInt(record[1]);
				pickUpCount += Integer.parseInt(record[2]);
				takeOffCount += Integer.parseInt(record[3]);
				speed += Float.parseFloat(record[5]);
				dir[Integer.parseInt(record[4])]++;
			}
			speed /= count;
			context.write(key, new Text(IDSet.size() + "," + freeCount + "," + pickUpCount + "," + takeOffCount + ","
					+ speed + "," + dir[0] + "," + dir[1] + "," + dir[2] + "," + dir[3] + "," + dir[4] + "," + dir[5]
					+ "," + dir[6] + "," + dir[7] + "," + dir[8]));
		}
	}

	// key = {latitude, longitude, time} value = {ID count, free count, pick up
	// count, take off count, avg speed, dirs}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path input = new Path("hdfs://blade42:9000/home/gaozhu/taxi-statistics-1");
		Path output = new Path("hdfs://blade42:9000/home/gaozhu/taxi-statistics-2");
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output))
			hdfs.delete(output, true);

		Job job = new Job(conf, "Taxi Statistics 2");
		job.setJarByClass(TaxiStatistics2.class);
		job.setMapperClass(TSMapper.class);
		job.setReducerClass(TSReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
