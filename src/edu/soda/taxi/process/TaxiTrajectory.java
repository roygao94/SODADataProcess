package edu.soda.taxi.process;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaxiTrajectory {

	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path inputDir = new Path("hdfs://blade42:9000/home/gaozhu/Taxi");
		Path outputDir = new Path("hdfs://blade42:9000/home/gaozhu/taxi-sorter");
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		
		Job job = new Job(conf, "TaxiSorter");
		job.setJarByClass(TaxiTrajectory.class);
		job.setMapperClass(TrajectoryMapper.class);
		job.setReducerClass(TrajectoryReducer.class);
		job.setNumReduceTasks(18);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class TrajectoryMapper extends Mapper<Object, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] record = line.split(",");
			outputKey.set(record[0]);
			// 2:free	8:lattitude	9:longtitude	10:speed	11:dir
			outputValue.set(record[7] + "," + record[2] + "," + record[8] + "," + record[9] + "," + record[10] + ","
					+ record[11]);
			context.write(outputKey, outputValue);

		}
	}

	static class KeyValue implements Comparable<KeyValue> {
		Date time;
		String info;

		public KeyValue(String time, String info) throws ParseException {
			this.time = dateFormat.parse(time);
			this.info = info;
		}

		@Override
		public int compareTo(KeyValue KV) {
			return time.compareTo(KV.time);
		}

		public String toString() {
			return dateFormat.format(time) + "," + info;
		}
	}

	public static class TrajectoryReducer extends Reducer<Text, Text, Text, Text> {
//		private Text outputKey = new Text();
//		private Text outputValue = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<KeyValue> timeLine = new ArrayList<>();
			for (Text val : values) {
				String[] split = val.toString().split(",", 2);
				try {
					timeLine.add(new KeyValue(split[0], split[1]));
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			Collections.sort(timeLine);
			for (KeyValue KV : timeLine)
				context.write(key, new Text(KV.toString()));
		}
	}
}
