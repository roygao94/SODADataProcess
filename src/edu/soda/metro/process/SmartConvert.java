package edu.soda.metro.process;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
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

import edu.soda.metro.io.WhenAndWhereAndPay;

/**
 * Created by Roy Gao on 10/7/2015.
 */
public class SmartConvert {

	public static class SmartMapper extends Mapper<Object, Text, Text, Text> {

		private Text head = new Text();
		private Text info = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			value = transformTextToUTF8(value, "GBK");
			String line = value.toString();
			String[] split = line.split(",");
			if (split[4].equals("地铁")) {
				head.set(split[0]);
				info.set(split[1] + ", " + split[2] + ", " + split[3] + ", " + split[5]);

				context.write(head, info);
			}
		}
	}

	public static Text transformTextToUTF8(Text text, String encoding) {
		String value = null;
		try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return new Text(value);
	}

	public static class SmartReducer extends Reducer<Text, Text, Text, Text> {

		private Text info = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// for (Text val : values)
			// context.write(key, val);
			List<WhenAndWhereAndPay> timeLine = new ArrayList<>();
			for (Text val : values) {
				String[] split = val.toString().split(",");
				String when = split[0] + split[1];
				String where = split[2];
				double pay = Double.parseDouble(split[3]);
				timeLine.add(new WhenAndWhereAndPay(when, where, pay));
			}
			Collections.sort(timeLine);

			String lastTime = "";
			String lastStation = "";
			double lastPay = -1;
//			int[] count = new int[4];
			for (WhenAndWhereAndPay pair : timeLine) {
				// context.write(key, new Text(pair.toString()));
				String time = pair.getWhen();
				String station = pair.getWhere();
				double pay = pair.getPay();

				if (pay == 0) {
					lastTime = time;
					lastStation = station;
					lastPay = 0;
				} else if (pay > 0 && lastPay == 0) {
					lastPay = -1;
					info.set(lastTime + ", " + time + "," + lastStation + ", " + station);
					context.write(key, info);
				}else if (pay == 0 && lastPay == 0) {//说明last只有进站没有出站
					
					
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Smart Convert");
		job.setJarByClass(SmartConvert.class);

		Path input = new Path("hdfs://blade42:9000/home/gaozhu/Smartcard");
		Path output = new Path("hdfs://blade42:9000/home/gaozhu/Smartcard-table1");

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output))
			hdfs.delete(output, true);

		job.setMapperClass(SmartMapper.class);
		job.setReducerClass(SmartReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
