package edu.soda.metro.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StationThroughput {

	public static HashMap<String, String> stationIndex = new HashMap<>();
	public static HashMap<String, String> pathIndex = new HashMap<>();
	public static Path inputPath = new Path("hdfs://blade42:9000/home/gaozhu/Smartcard-table1/part-r-00000");
	public static Path outputPath = new Path("hdfs://blade42:9000/home/gaozhu/metroThroughput/");

	public static class StationConvertMapper extends Mapper<Object, Text, Text, Text> {

		public void setup(Context context) throws IOException {

			Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (caches.length < 2) {
				System.err.println("Input Files are missing!");
				System.exit(1);
			}
			getStationIndex(caches[0].toString());
			getPathIndex(caches[1].toString());

		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = new String();
			Text outputKey = new Text();
			Text outputValue = new Text();
			Date enterDate = new Date();
			Date exitDate = new Date();
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			line = value.toString();
			String[] keyValue = line.split("\t");
			String[] userInfo = keyValue[1].split(",\\s*");
			if (userInfo.length != 4)
				System.out.println("Error record:" + keyValue[1]);
			userInfo[2] = userInfo[2].replaceAll(" ", "").replaceAll("号线", "");
			userInfo[3] = userInfo[3].replaceAll(" ", "").replaceAll("号线", "");
			if (!userInfo[2].equals(userInfo[3])) {
				String[] stationID = getLine(userInfo[2], userInfo[3]).split(",");
				try {
					enterDate = format.parse(userInfo[0]);
					enterDate.setSeconds(0);
					enterDate.setMinutes(enterDate.getMinutes() / 5 * 5);
					exitDate = format.parse(userInfo[1]);
					exitDate.setSeconds(0);
					exitDate.setMinutes(exitDate.getMinutes() / 5 * 5);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				if (stationID[0].length() < 2 || stationID[1].length() < 2)
					System.out.println(stationID[0] + "," + stationID[1]);
				int railIn = Integer.parseInt(stationID[0].substring(0, 2));
				int railOut = Integer.parseInt(stationID[1].substring(0, 2));
				outputKey.set(stationID[0] + "," + format.format(enterDate));
				int i;
				for (i = 0; Character.isDigit(userInfo[2].charAt(i)); ++i)
					;
				String name1 = userInfo[2].substring(i);
				outputValue.set("1," + railIn + "号线" + name1);
				// System.out.println("In:" + railIn + "号线" + userInfo[2]);
				context.write(outputKey, outputValue);
				outputKey.set(stationID[1] + "," + format.format(exitDate));
				for (i = 0; Character.isDigit(userInfo[3].charAt(i)); ++i)
					;
				String name2 = userInfo[3].substring(i);
				outputValue.set("2," + railOut + "号线" + name2);
				// System.out.println("Out:" + railOut + "号线" + userInfo[3]);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class AggregationReducer extends Reducer<Text, Text, Text, Text> {

		private Text count = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int[] sum = { 0, 0, 0 };
			String stationName = new String();
			for (Text val : values) {
				String[] split = val.toString().split(",");
				stationName = split[1];
				if (split[0].equals("1"))
					sum[0]++;
				else if (split[0].equals("2"))
					sum[1]++;
				else {
					System.out.println("Error record: " + val.toString());
					sum[2]--;
				}
				sum[2]++;
			}
			count.set(stationName + "," + sum[0] + "," + sum[1] + "," + sum[2]);

			context.write(key, count);
		}
	}

	private static void getStationIndex(String fileName) {
		File file = new File(fileName);
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GBK"));
			String inputLine = br.readLine();
			while ((inputLine = br.readLine()) != null) {
				String[] strings = inputLine.split(",");
				if (strings[0].length() == 3)
					strings[0] = "0" + strings[0];
				int line = Integer.parseInt(strings[0].substring(0, 2));
				stationIndex.put(strings[0], line + strings[1]);
				// System.out.println(strings[0] + strings[1]);
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

	private static void getPathIndex(String fileName) {
		File file = new File(fileName);
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
			String inputLine = "";
			while ((inputLine = br.readLine()) != null) {
				String[] strings = inputLine.split(":");
				pathIndex.put(strings[0], strings[1]);
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

	public static String getPath(String departFrom, String destination) {
		String res = "";
		if (pathIndex.get(departFrom + "-" + destination) != null) {
			res = pathIndex.get(departFrom + "-" + destination);
		}
		if (res == "") {
			System.err.println("Path not Found:" + departFrom + "-" + destination);
		}
		return res;
	}

	public static String getLine(String start, String end) {
		String res = "";
		// System.out.println("Size: " + stationIndex.size());
		if ((!stationIndex.containsValue(start)) || (!stationIndex.containsValue(end))) {
			System.out.println(start + "," + end);
			return null;
		}
		// System.out.println("input: " + start + "," + end);
		String path = getPath(start, end);
		String[] stations = path.split(",");
		for (int i = 0; i < stations.length; ++i) {
			if (stations[i].length() == 3)
				stations[i] = "0" + stations[i];
			else if (stations[i].length() != 4)
				System.out.println("ErrorStation:" + stations[i]);
		}
		res = stations[0] + ",";
		if (stations.length - 2 >= 0) {
			String temp = stations[stations.length - 2].substring(0, 2);
			if (!temp.equals(stations[stations.length - 1].substring(0, 2))) {
				res += stations[stations.length - 2];
			} else {
				res += stations[stations.length - 1];
			}
		} else {
			res += stations[stations.length - 1];
		}

		// System.out.println("station:" + res);
		return res;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputPath, true);

		// get distributed file path
		Path stationOriginal = new Path("hdfs://blade42:9000/home/gaozhu/StationInfo/StationOriginal.csv");
		Path shortestPath = new Path("hdfs://blade42:9000/home/gaozhu/StationInfo/ShortestPath.txt");
		DistributedCache.addCacheFile(stationOriginal.toUri(), conf);
		DistributedCache.addCacheFile(shortestPath.toUri(), conf);

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 0) {
			System.err.println("Usage: No arg needed!");
			System.exit(2);
		}
		Job job = new Job(conf, "Station Throughput Count");

		job.setJarByClass(StationThroughput.class);
		job.setMapperClass(StationConvertMapper.class);
		job.setReducerClass(AggregationReducer.class);

		// job.setNumReduceTasks(18);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
