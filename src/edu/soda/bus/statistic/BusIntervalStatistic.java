package edu.soda.bus.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.soda.bus.io.BusInputRecord;
import edu.soda.bus.io.BusRecord;
import edu.soda.bus.io.BusRecordKey;

public class BusIntervalStatistic {

	public static HashMap<String, Integer> stationNum = new HashMap<String, Integer>();
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static class InputMapper extends Mapper<Object, Text, Text, BusInputRecord> {

		private Text outputKey = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] record = line.split(",");
			if (record.length == 7 && Character.isDigit(record[1].charAt(0))) {
				outputKey.set(record[1] + "," + record[0]);
				BusInputRecord outputValue;
				if (record[4].equals("进站") || record[4].equals("出站"))
					if (record[5].equals("上行") || record[5].equals("下行")) {
						try {
							outputValue = new BusInputRecord(record[1], record[2], (record[5].equals("上行") ? 1 : 2),
									format.parse(record[6]), (record[4].equals("进站") ? true : false));
							System.out.println(outputValue.getBusID() + "," + outputValue.getCurrentStation() + ","
									+ outputValue.getDirection() + "," + outputValue.getStatus() + ","
									+ outputValue.getArrivingTime());
							System.out.println(outputKey);
							context.write(outputKey, outputValue);
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
			} else
				System.out.println("An Error Record Occurred:" + line);
		}
	}

	public static class JoinReducer extends Reducer<Text, BusInputRecord, LongWritable, BusRecord> {

		private LongWritable outputKey = new LongWritable();

		public void setup(Context context) throws IOException {

			Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (caches.length != 1) {
				System.err.println("Input file error!");
				System.exit(1);
			}
			getStationIndex(caches[0]);
		}

		public void reduce(Text key, Iterable<BusInputRecord> values, Context context) throws IOException,
				InterruptedException {

			List<BusInputRecord> list = new ArrayList<BusInputRecord>();

			if (key.toString().split(",")[0].equals("")) {
				System.out.println("Error Record:" + key);
				return;
			}
			// Iterator<BusInputRecord> iter = values.iterator();
			//
			// while (iter.hasNext()) {
			// list.add(iter.next());
			// System.out.println(list.get(0).getCurrentStation() + "," +
			// list.get(0).getDirection() + "," +
			// list.get(0).getArrivingTime());
			// }
			for (BusInputRecord val : values) {
				BusInputRecord elem = new BusInputRecord(val.getBusID(), val.getCurrentStation(), val.getDirection(),
						val.getArrivingTime(), val.getStatus());
				list.add(elem);
			}
			Collections.sort(list);
			String previousStation = "";
			Date arriveTime = new Date();
			int previousStationNum = 0;
			int previousDirection = 0;
			System.out.println(list.size());
			// Iterator<BusInputRecord> iter = list.iterator();
			//
			// while (iter.hasNext()) {
			// BusInputRecord record = iter.next();
			for (BusInputRecord record : list) {
				System.out.println(record.getDirection() + "," + record.getStatus() + "," + record.getCurrentStation()
						+ "," + format.format(record.getArrivingTime()));
				if (stationNum.containsKey(record.getBusID() + "," + record.getDirection() + ","
						+ record.getCurrentStation())) {
					if (previousStation.equals("") || previousStationNum == 0 || previousDirection == 0) {
						if (record.getStatus() == false) {
							previousStation = record.getCurrentStation();
							arriveTime = record.getArrivingTime();
							previousStationNum = stationNum.get(record.getBusID() + "," + record.getDirection() + ","
									+ record.getCurrentStation());
							previousDirection = record.getDirection();
							System.out.println("test:" + previousStation + "," + previousStationNum + ","
									+ previousDirection);
						}
					} else {
						if (record.getStatus() == true) {
							int currentStationNum = stationNum.get(record.getBusID() + "," + record.getDirection()
									+ "," + record.getCurrentStation());
							System.out.println("Gogogo:" + currentStationNum + "," + previousStationNum);
							if (currentStationNum == previousStationNum + 1
									&& record.getDirection() == previousDirection) {
								long duration = (record.getArrivingTime().getTime() - arriveTime.getTime()) / 1000;
								System.out.println(record.getBusID() + "," + previousStation + ","
										+ record.getCurrentStation() + "," + previousStationNum + ","
										+ currentStationNum + "," + record.getDirection() + ","
										+ format.format(arriveTime) + "," + duration);
								BusRecord outputValue = new BusRecord(record.getBusID(), previousStation,
										record.getCurrentStation(), previousStationNum, currentStationNum,
										record.getDirection(), arriveTime, duration);
								outputKey.set(Long.parseLong(record.getBusID()));
								context.write(outputKey, outputValue);
							} else
								System.out.println("Error station Number:" + previousStationNum + ","
										+ currentStationNum);
						} else if (record.getStatus() == false) {
							previousStation = record.getCurrentStation();
							arriveTime = record.getArrivingTime();
							previousStationNum = stationNum.get(record.getBusID() + "," + record.getDirection() + ","
									+ record.getCurrentStation());
							previousDirection = record.getDirection();
							System.out.println("WTF:" + previousStation + "," + previousStationNum + ","
									+ previousDirection);
						}
					}
					// } else {
					// continue;
					// System.out.println("Error Record:" + val.getBusID() + ","
					// + val.getDirection() + ","
					// + val.getCurrentStation());
				}
			}
		}
	}

	public static class ReformMapper extends Mapper<LongWritable, BusRecord, BusRecordKey, BusRecord> {

		public void map(LongWritable key, BusRecord value, Context context) throws IOException, InterruptedException {
			BusRecordKey outputKey = new BusRecordKey(key.toString(), value.getPreviousStation(),
					value.getCurrentStation(), value.getPreviousStationNum(), value.getDirection());
			context.write(outputKey, value);
		}
	}

	public static class BusOutputReducer extends Reducer<BusRecordKey, BusRecord, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void reduce(BusRecordKey key, Iterable<BusRecord> values, Context context) throws IOException,
				InterruptedException {

			List<BusRecord> list = new ArrayList<BusRecord>();

			for (BusRecord val : values) {
				BusRecord elem = new BusRecord(val.getBusID(), val.getPreviousStation(), val.getCurrentStation(),
						val.getPreviousStationNum(), val.getCurrentStationNum(), val.getDirection(),
						val.getLeavingTime(), val.getDuration());
				list.add(elem);
			}
			Collections.sort(list);

			for (BusRecord val : list) {
				if (!val.getBusID().equals(key.getBusID()))
					System.out.println("It's weird!" + val.getBusID() + "," + key.toString().split(",")[0]);
				outputKey.set(val.getBusID());
				outputValue.set(val.getDirection() + "," + val.getPreviousStationNum() + "," + val.getPreviousStation()
						+ "-" + val.getCurrentStation() + "," + format.format(val.getLeavingTime()) + ","
						+ val.getDuration());
				context.write(outputKey, outputValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path inputPath = new Path("hdfs://blade42:9000/home/gaozhu/Bus/BusRecords");
		Path firstOutputPath = new Path("hdfs://blade42:9000/home/gaozhu/bus-record/");
		Path secondInputPath = new Path("hdfs://blade42:9000/home/gaozhu/bus-record/part-r-00000");
		Path outputPath = new Path("hdfs://blade42:9000/home/gaozhu/bus-statistic/");
		Path distributedFilePath = new Path("hdfs://blade42:9000/home/gaozhu/Bus/BusLine");

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 0) {
			System.err.println("Usage: No arg needed!");
			System.exit(2);
		}
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(firstOutputPath))
			hdfs.delete(firstOutputPath, true);
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);

		DistributedCache.addCacheFile(distributedFilePath.toUri(), conf);

		Job job = new Job(conf, "Bus Input Proccess");

		job.setJarByClass(BusIntervalStatistic.class);
		job.setMapperClass(InputMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BusInputRecord.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BusRecord.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// job.setNumReduceTasks(17);
		FileInputFormat.addInputPath(job, inputPath);
		SequenceFileOutputFormat.setOutputPath(job, firstOutputPath);
		if (job.waitForCompletion(true)) {

			Job secondJob = new Job(conf, "Bus Statistic");

			SequenceFileInputFormat.addInputPath(secondJob, secondInputPath);
			FileOutputFormat.setOutputPath(secondJob, outputPath);

			secondJob.setInputFormatClass(SequenceFileInputFormat.class);
			secondJob.setJarByClass(BusIntervalStatistic.class);
			secondJob.setMapperClass(ReformMapper.class);
			secondJob.setReducerClass(BusOutputReducer.class);
			secondJob.setMapOutputKeyClass(BusRecordKey.class);
			secondJob.setMapOutputValueClass(BusRecord.class);
			secondJob.setOutputKeyClass(Text.class);
			secondJob.setOutputValueClass(Text.class);
			System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
		}
	}

	public static void getStationIndex(Path stationInfo) {
		File file = new File(stationInfo.toString());
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
			int series = 0;
			String line = "";
			while ((line = br.readLine()) != null) {
				line.replaceAll(" ", "");
				String[] split = line.split(":");
				String[] station = split[1].split(",");
				for (int i = 0; i < station.length; i++) {
					if (!stationNum.containsKey(split[0] + "," + station[i])) {
						series++;
						stationNum.put(split[0] + "," + station[i], series);
						System.err.println(split[0] + "," + station[i] + "," + series);
					} else {
						System.out.println("Key Exist:" + split[0] + "," + station[i]);
						series--;
					}
				}
				series = 0;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null) {
					br.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
