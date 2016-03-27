package edu.soda.taxi.statistic;

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

public class TaxiStatistic {

	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path inputDir = new Path("hdfs://blade42:9000/home/gaozhu/Taxi/");
		Path outputDir = new Path("hdfs://blade42:9000/home/gaozhu/taxi-statistics-1");
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		Job job = new Job(conf, "TaxiSorter");
		job.setJarByClass(TaxiStatistic.class);
		job.setMapperClass(TrajectoryMapper.class);
		job.setReducerClass(TrajectoryReducer.class);
		job.setNumReduceTasks(17);
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
			// 2:free 8:latitude 9:longitude 10:speed 11:dir
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
		// private Text outputKey = new Text();
		// private Text outputValue = new Text();
		private final long oneHour = 1000 * 60 * 60;

		// output : key = Id value = {time, free, pick up, take off, latitude,
		// longitude, dir, speed}
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<KeyValue> timeLine = new ArrayList<>();
			for (Text val : values) {
				String[] split = val.toString().split(",", 2);
				try {
					if (split[0].length() < 19)
						continue;
					timeLine.add(new KeyValue(split[0], split[1]));
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			if (timeLine.size() == 0) {
				System.out.println(key.toString() + " timeLine is empty!");
				return;
			}

			Collections.sort(timeLine);
			int pickUp = 0, takeOff = 0;

			Date lastTime = timeLine.get(0).time;
			String[] rec = timeLine.get(0).info.split(",");
			float speed = Float.parseFloat(rec[3]);
			float lastSpeed = speed;
			int count = 1;
			int sleep = 0;
			int free = Integer.parseInt(rec[0]);
			float tmp = Float.parseFloat(rec[1]);
			float latitude = 120 + ((int) ((tmp - 120) * 10000) / 300 * 300 + 150) / (10000 * 1f);
			tmp = Float.parseFloat(rec[2]);
			float longitude = 30 + ((int) ((tmp - 30) * 10000) / 300 * 300 + 150) / (10000 * 1f);
			if (free == 1 && Float.parseFloat(rec[3]) < 10f)
				sleep++;
			for (int i = 1; i < timeLine.size(); ++i) {
				KeyValue KV = timeLine.get(i);
				Date time = KV.time;
				rec = KV.info.split(",");
				int free1 = 0;
				try {
					free1 = Integer.parseInt(rec[0]);
				} catch (NumberFormatException e) {
					System.out.println(key.toString() + " free status is not a number!");
				}
				tmp = Float.parseFloat(rec[1]);
				float latitude1 = 120 + ((int) ((tmp - 120) * 10000) / 300 * 300 + 150) / (10000 * 1f);
				tmp = Float.parseFloat(rec[2]);
				float longitude1 = 30 + ((int) ((tmp - 30) * 10000) / 300 * 300 + 150) / (10000 * 1f);

				if (time.getTime() - lastTime.getTime() < oneHour) {
					if (latitude1 == latitude && longitude1 == longitude && free == 1 && Float.parseFloat(rec[3]) < 10f
							&& lastSpeed < 10f) {
						sleep++;
						if (sleep >= 720){
							lastTime = time;
							free = free1;							
							continue;
						}
					} else
						sleep = 0;
					if (time.getMinutes() / 5 * 5 == lastTime.getMinutes() / 5 * 5) {
						if (latitude1 != latitude || longitude1 != longitude || free != free1) {
							if (free > free1)
								pickUp++;
							else if (free < free1)
								takeOff++;
							lastTime.setMinutes(lastTime.getMinutes() / 5 * 5);
							lastTime.setSeconds(0);
							context.write(
									key,
									new Text(dateFormat.format(lastTime) + "," + free + "," + pickUp + "," + takeOff
											+ "," + latitude + "," + longitude + ","
											+ getDirection(latitude, longitude, latitude1, longitude1) + "," + speed
											/ count));
							// int direction = getDirection(latitude, longitude,
							// latitude1, longitude1);
							pickUp = takeOff = 0;
							speed = count = 0;
						}
					} else {
						if (free > free1)
							pickUp++;
						else if (free < free1)
							takeOff++;
						lastTime.setMinutes(lastTime.getMinutes() / 5 * 5);
						lastTime.setSeconds(0);
						context.write(key, new Text(dateFormat.format(lastTime) + "," + free + "," + pickUp + ","
								+ takeOff + "," + latitude + "," + longitude + "," + 8 + "," + speed / count));
						pickUp = takeOff = 0;
						speed = count = 0;
					}
					if (Float.parseFloat(rec[3]) <= 250f) {
						speed += Float.parseFloat(rec[3]);
						count++;
					}
				}
				lastTime = time;
				free = free1;
				latitude = latitude1;
				longitude = longitude1;
			}
		}

		public int getDirection(float x, float y, float x1, float y1) {
			int dx, dy;
			if (x1 == x)
				dx = 0;
			else if (x1 > x)
				dx = 1;
			else
				dx = -1;
			if (y1 == y)
				dy = 0;
			else if (y1 > y)
				dy = 1;
			else
				dy = -1;

			if (dx == -1) {
				if (dy == -1)
					return 7;
				else if (dy == 0)
					return 0;
				else if (dy == 1)
					return 1;
			} else if (dx == 0) {
				if (dy == -1)
					return 6;
				else if (dy == 1)
					return 2;
			} else if (dx == 1) {
				if (dy == -1)
					return 5;
				else if (dy == 0)
					return 4;
				else if (dy == 1)
					return 3;
			}
			return 0;
		}
	}
}
