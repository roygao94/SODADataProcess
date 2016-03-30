package edu.soda.metro.statistic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.soda.metro.io.WhenAndWhere;

/**
 * Created by Roy Gao on 10/9/2015.
 */
public class MetroStatistics {

	public static class MetroMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Text head = new Text();
		private IntWritable direction = new IntWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t");
			String[] timeLine = split[1].split("->");

			for (String str : timeLine) {
				WhenAndWhere waw = new WhenAndWhere(str);
				head.set(waw.getID() + "," + waw.getName() + "," + waw.getTimeStr());
				direction.set(waw.getDirection());
				context.write(head, direction);
			}
		}
	}

	public static class MetroReducer extends Reducer<Text, IntWritable, Text, Text> {

		private Text count = new Text();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int[] sum = {0, 0, 0, 0};
			for (IntWritable val : values) {
				int direction = val.get();
				sum[direction]++;
			}
			count.set(sum[0] + "," + sum[1] + "," + sum[2] + "," + sum[3]);

			context.write(key, count);
		}
	}

	public static class IDMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			context.write(new Text(split[0]), new Text(split[1]));
		}
	}

	public static class SecondReducer extends Reducer<Text, Text, Text, Text> {

		private Text count = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			int[] sum = {0, 0, 0, 0};
			for (Text val : values) {
				String[] split = val.toString().split(",");
				for (int i = 0; i < 4; ++i)
					sum[i] += Integer.parseInt(split[i]);
			}
			count.set(sum[0] + "," + sum[1] + "," + sum[2] + "," + sum[3]);

			context.write(key, count);
		}
	}

	public static void main(String[] args) throws Exception {
		String[] ending = {"00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17"};
		for (String str : ending) {
			Configuration conf = new Configuration();
			Path inputPath = new Path("hdfs://blade42:9000/home/gaozhu/metro3/part-r-000" + str);
			Path outputPath = new Path("hdfs://blade42:9000/home/gaozhu/metro3-statistics/" + str);
			FileSystem hdfs = FileSystem.get(conf);
			if (hdfs.exists(outputPath))
				hdfs.delete(outputPath, true);

			Job job = new Job(conf, "Metro Statistics");

			job.setJarByClass(MetroStatistics.class);
			job.setMapperClass(MetroMapper.class);
			job.setReducerClass(MetroReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputPath);
			if (job.waitForCompletion(true))
				continue;
		}

		Configuration conf = new Configuration();
		Path outputPath = new Path("hdfs://blade42:9000/home/gaozhu/metro3-statistics-combine/");
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);

		Job job = new Job(conf, "Metro Statistics");

		job.setJarByClass(MetroStatistics.class);
		job.setMapperClass(IDMapper.class);
		job.setCombinerClass(SecondReducer.class);
		job.setReducerClass(SecondReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (String str : ending) {
			Path inputPath = new Path("hdfs://blade42:9000/home/gaozhu/metro3-statistics/" + str + "/part-r-00000");
			FileInputFormat.addInputPath(job, inputPath);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
