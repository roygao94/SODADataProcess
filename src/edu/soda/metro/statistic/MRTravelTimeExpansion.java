package edu.soda.metro.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

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

import edu.soda.metro.io.WhenAndWhere;

public class MRTravelTimeExpansion {

	public static HashMap<String, Integer> metroInter = new HashMap<>();
	public static HashMap<String, String> stationIndex = new HashMap<>();
	public static HashMap<String, Integer> intervalHash = new HashMap<>();
	public static HashMap<String, String> pathIndex = new HashMap<>();
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static SimpleDateFormat dateFormat3 = new SimpleDateFormat("yyyy-MM-dd,HH:mm:ss");
	private static Path inputPath = new Path("hdfs://blade42:9000/home/gaozhu/Smartcard-output/part-r-00000");
	private static Path outputPath = new Path("hdfs://blade42:9000/home/gaozhu/metro3/");

	public static class TravelTimeMapper extends Mapper<Object, Text, Text, Text> {

		public void setup(Context context) throws IOException {

			Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (caches.length < 4) {
				System.err.println("Input Files are missing!");
				System.exit(1);
			}
			getTrainInterval(caches[0].toString());
			getStationIndex(caches[1].toString());
			getIntervalInfo(caches[2].toString());
			getPathIndex(caches[3].toString());

		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String keyValue, ID, timeStr, departFrom, destination;
			int count = 0;
			String[] line = value.toString().split("\t");
			ID = line[0];
			String[] split = line[1].split(",\\s*");
			if (split.length != 3) {
				System.out.println("Error record");
				return;
			}
			Date time;
			try {
				time = dateFormat2.parse(split[0]);
				departFrom = split[1].replaceAll(" ", "");
				destination = split[2].replaceAll(" ", "");
				departFrom = departFrom.replaceAll("号线", "");
				destination = destination.replaceAll("号线", "");

				int i;
				for (i = 0; Character.isDigit(departFrom.charAt(i)); ++i)
					;
				String name1 = departFrom.substring(i);
				for (i = 0; Character.isDigit(destination.charAt(i)); ++i)
					;
				String name2 = destination.substring(i);

				if (!name1.equals(name2)) {
					List<WhenAndWhere> timeLine = getTravelTime(time, departFrom, destination);
					if (timeLine == null)
						System.out.println("Error Path");
					else {
						String route = "";
						for (int j = 0; j < timeLine.size(); ++j) {
							if (j > 0)
								route += "->";
							route += timeLine.get(j).toString();
						}
						context.write(new Text(ID), new Text(route));
					}
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
			// timeStr = dateFormat.format(time);
		}
	}

	public static class IdentityReducer extends Reducer<Text, Text, Text, Text> {

		private Text count = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(key, val);
			}
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
				System.out.println(strings[0] + "," + line + strings[1]);
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

	private static void getTrainInterval(String intervalFile) {
		File file = new File(intervalFile);
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GBK"));
			String inputLine = "";
			int lineNumber = 1;
			String time = "";
			String day = "";
			String rail = "";
			Integer value = 0;
			String[] railWay = new String[] { "101", "111", "201", "211", "221", "301", "302", "311", "312", "401",
					"402", "501", "601", "602", "611", "612", "701", "702", "711", "712", "801", "811", "821", "901",
					"902", "911", "912", "1001", "1011", "1101", "1111", "1121", "1201", "1211", "1301", "1601",
					"1602", "1611", "1612" };
			while ((inputLine = br.readLine()) != null) {
				if (lineNumber > 114)
					System.out.println("Table read Error");
				String[] para = inputLine.split(",");
				if (lineNumber < 39)
					day = "1";
				else if (lineNumber < 77)
					day = "5";
				else
					day = "6";
				time = para[0];
				for (int i = 1; i <= 39; i++) {
					rail = railWay[i - 1];
					value = Integer.valueOf(para[i]);
					metroInter.put(day + time + rail, value);
					System.out.println(day + time + rail + "," + value);
				}
				// System.out.println(lineNumber);
				lineNumber++;
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

	private static void getIntervalInfo(String fileName) {
		File file = new File(fileName);
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GBK"));
			String inputLine = br.readLine();
			while ((inputLine = br.readLine()) != null) {
				String[] strings = inputLine.split(",");
				for (int i = 1; i < strings.length; i++) {
					String[] strs = strings[i].split("#");
					intervalHash.put(strs[0] + "-" + strs[1], Integer.parseInt(strs[2]));
					System.out.println(strs[0] + "-" + strs[1] + "," + Integer.parseInt(strs[2]));
					intervalHash.put(strs[1] + "-" + strs[0], Integer.parseInt(strs[2]));
				}
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
				System.out.println(strings[0] + "," + strings[1]);
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

	// 根据起始站、目的地获取路线
	public static String getPath(String departFrom, String destination) {
		String res = "";
		if (pathIndex.get(departFrom + "-" + destination) != null) {
			res = pathIndex.get(departFrom + "-" + destination);
		}
		if (res == "") {
			System.out.println("Path not Found:" + departFrom + "-" + destination);
		}
		return res;
	}

	// 根据站点ID获取线路线
	public static String getLine(String start, String end) {
		String res = "";
		// System.out.println("Size: " + stationIndex.size());
		if ((!stationIndex.containsValue(start)) || (!stationIndex.containsValue(end))) {
			System.err.println(start + "," + end);
			return null;
		}
		String path = getPath(start, end);
		String[] stations = path.split(",");
		for (int i = 0; i < stations.length; ++i)
			if (stations[i].length() < 4)
				stations[i] = "0" + stations[i];
		res = stations[0] + ",";
		String temp = stations[stations.length - 2].substring(0, 2);
		if (!temp.equals(stations[stations.length - 1].substring(0, 2))) {
			res += stations[stations.length - 2];
		} else {
			res += stations[stations.length - 1];
		}
		return res;
	}

	public static List<WhenAndWhere> getTravelTime(Date enterTime, String departFrom, String destination) {

		// 站点列表中找不到起始站点、目的地站点名直接退出
		if (!stationIndex.containsValue(departFrom)) {
			System.out.println(departFrom + "not found!");
			return null;
		}
		if (!stationIndex.containsValue(destination)) {
			System.out.println(destination + "not found!");
			return null;
		}

		// timeLine用来记录到达线路上的每一站的时间
		List<WhenAndWhere> timeLine = new ArrayList<>();
		String path = getPath(departFrom, destination);
		String[] stations = path.split(",");
		for (int i = 0; i < stations.length; ++i)
			if (stations[i].length() < 4)
				stations[i] = "0" + stations[i];
		int shangXia = 0; // 0-上行，1-下行， 2-上行等待，3-下行等待
		int wait = 0;
		boolean b = false; // b用来判断上一站是否换乘
		Date start = enterTime;

		for (int i = 0; i < stations.length; i++) {
			if (stations[i].length() == 3) {
				stations[i] = "0" + stations[i];
			}
			if (i == 0 || (b == true)) {
				b = false;
				// System.out.println(stations[i]+ "\t" + stations[i + 1] + "\t"
				// + isSameLine(stations[i], stations[i + 1]));
				if (isSameLine(stations[i], stations[i + 1])) {
					boolean temp = false; // 记录上下行
					if (Integer.parseInt(stations[i]) < Integer.parseInt(stations[i + 1])) {// 线路号不变，站点ID上升，上行
						shangXia = 0; // 上行
						temp = true;
						if (stations[i].equals("0401") && stations[i + 1].equals("0426")) {
							// 4号线环线
							shangXia = 1;
							temp = false;
						}
					} else {
						shangXia = 1;// 下行
						temp = false;
						if (stations[i].equals("0426") && stations[i + 1].equals("0401")) {
							// 4号线环线
							shangXia = 0;
							temp = true;
						}
					}
					// 一开始进站计算等车时间，如果换乘直接算3分钟
					if (i == 0) {
						// System.out.println("waiting...");
						wait = intervalTime(enterTime, stations[i], temp);
					} else {
						wait = 3;
					}

				}
			} else {
				if (isSameLine(stations[i - 1], stations[i])) {// 没有换乘
					wait = getIntervalTime(stationIndex.get(stations[i - 1]), stationIndex.get(stations[i]));// ��ȡ2վ�ļ��ʱ�䣻

				} else {// 换乘
					if (i < stations.length - 1) {
						i--;
						b = true;
						continue;
					}
				}
			}
			enterTime = getTime(enterTime, wait);
			WhenAndWhere wAndw = new WhenAndWhere(enterTime, stations[i], stationIndex.get(stations[i]), shangXia);
			timeLine.add(wAndw);
		}

		adjustTime(timeLine);
		addWaitingTime(timeLine, start);

		return timeLine;
	}

	// 在时间线开头加上等车时间
	private static void addWaitingTime(List<WhenAndWhere> timeLine, Date start) {
		Date time = start;
		String date = dateFormat.format(time);
		int hour = time.getHours();
		int minute = time.getMinutes();
		minute = minute / 5 * 5;
		Date newStart;
		List<WhenAndWhere> waitingList = new ArrayList<>();
		try {
			if (minute > 9)
				newStart = dateFormat3.parse(date + "," + hour + ":" + minute + ":00");
			else
				newStart = dateFormat3.parse(date + "," + hour + ":0" + minute + ":00");
			Date aboardTimeDate = timeLine.get(0).getTime();
			int direction = timeLine.get(0).getDirection() == 0 ? 2 : 3; // 2-上行等待，3-下行等待
			String ID = timeLine.get(0).getID();
			String name = timeLine.get(0).getName();
			// 按5分钟累计
			while (newStart.before(aboardTimeDate)) {
				waitingList.add(new WhenAndWhere(newStart, ID, name, direction));
				newStart = getTime(newStart, 5);
			}
			timeLine.addAll(0, waitingList);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 将时间线中的时间信息按5分钟向前靠
	private static void adjustTime(List<WhenAndWhere> timeLine) {
		for (WhenAndWhere waw : timeLine) {
			Date time = waw.getTime();
			String date = dateFormat.format(time);
			int hour = time.getHours();
			int minute = time.getMinutes();
			minute = minute / 5 * 5;
			Date newDate;
			try {
				if (minute > 9)
					newDate = dateFormat3.parse(date + "," + hour + ":" + minute + ":00");
				else
					newDate = dateFormat3.parse(date + "," + hour + ":0" + minute + ":00");
				waw.setTime(newDate);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// 根据在上一站的时间和中间坐地铁或等地铁时间计算到下一站的时间
	private static Date getTime(Date enterTime, int wait) {
		String date = dateFormat.format(enterTime);
		int hour = enterTime.getHours();
		int minute = enterTime.getMinutes();
		minute += wait;
		if (minute > 59) {
			minute -= 60;
			hour++;
		}
		Date newDate = null;
		try {
			if (minute > 9)
				newDate = dateFormat3.parse(date + "," + hour + ":" + minute + ":00");
			else
				newDate = dateFormat3.parse(date + "," + hour + ":0" + minute + ":00");
		} catch (Exception e) {
			e.printStackTrace();
		}

		return newDate;
	}

	public static boolean isSameLine(String pre, String after) {
		if (pre.length() == after.length()) {
			if (pre.length() == 3) {
				if (pre.substring(0, 1).equals(after.substring(0, 1))) {
					return true;
				}
			}
			if (pre.length() == 4) {
				if (pre.substring(0, 2).equals(after.substring(0, 2))) {
					return true;
				}
			}
		}
		return false;
	}

	public static int intervalTime(Date date, String stationID, Boolean dir) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int dayForWeek = 0;
		if (cal.get(Calendar.DAY_OF_WEEK) == 1) {
			dayForWeek = 7;
		} else {
			dayForWeek = cal.get(Calendar.DAY_OF_WEEK) - 1;
		}
		if (dayForWeek == 0)
			System.out.println("Date Error");
		else if (dayForWeek <= 4)
			dayForWeek = 1;
		else if (dayForWeek > 5)
			dayForWeek = 6;
		String day = dayForWeek + "";

		SimpleDateFormat format = new SimpleDateFormat("HH");
		String time = format.format(date).toString();
		if (time.equals("00") || time.equals("01") || time.equals("02") || time.equals("03") || time.equals("04"))
			time = "05";
		if (date.getMinutes() >= 30)
			time += "30";
		else
			time += "00";

		String rail = "";
		int line = 0;
		int station = 0;
		if (stationID.length() == 3)
			line = Integer.parseInt(stationID.substring(0, 1));
		else
			line = Integer.parseInt(stationID.substring(0, 2));
		station = Integer.parseInt(stationID);
		switch (line) {
		case 1: {
			if (station < 126)
				rail = "101";
			else if (station == 126)
				rail = dir ? "111" : "101";
			else
				rail = "111";
			break;
		}
		case 2: {
			if (station < 237)
				rail = "201";
			else if (station == 237)
				rail = dir ? "211" : "201";
			else if (station < 255)
				rail = "211";
			else if (station == 255)
				rail = dir ? "221" : "211";
			else
				rail = "221";
			break;
		}
		case 3: {
			if (station < 331)
				rail = dir ? "301" : "302";
			else if (station == 331)
				rail = dir ? "311" : "302";
			else
				rail = dir ? "311" : "312";
			break;
		}
		case 4: {
			rail = dir ? "401" : "402";
			break;
		}
		case 5: {
			rail = "501";
			break;
		}
		case 6: {
			if (station > 625 && station < 641)
				rail = dir ? "611" : "612";
			else if (station == 625)
				rail = dir ? "611" : "602";
			else if (station == 641)
				rail = dir ? "601" : "612";
			else
				rail = dir ? "601" : "602";
			break;
		}
		case 7: {
			if (station < 726)
				rail = dir ? "701" : "702";
			else if (station == 726)
				rail = dir ? "711" : "702";
			else
				rail = dir ? "711" : "712";
			break;
		}
		case 8: {
			if (station < 826)
				rail = "801";
			else if (station == 826)
				rail = dir ? "811" : "801";
			else if (station < 845)
				rail = "811";
			else if (station == 845)
				rail = dir ? "821" : "811";
			else
				rail = "821";
			break;
		}
		case 9: {
			if (station < 924)
				rail = dir ? "901" : "902";
			else if (station == 924)
				rail = dir ? "911" : "902";
			else
				rail = dir ? "911" : "912";
			break;
		}
		case 10: {
			if (station < 1045)
				rail = "1001";
			else if (station == 1045)
				rail = dir ? "1011" : "1001";
			else
				rail = "1011";
			break;
		}
		case 11: {
			if (station > 1134 && station < 1137)
				rail = "1111";
			else if (station == 1134)
				rail = dir ? "1111" : "1101";
			else if (station == 1137)
				rail = dir ? "1121" : "1111";
			else if (station > 1137 && station < 1155)
				rail = "1121";
			else if (station == 1155)
				rail = dir ? "1101" : "1121";
			else
				rail = "1101";
			break;
		}
		case 12: {
			if (station < 1247)
				rail = "1201";
			else if (station == 1247)
				rail = dir ? "1211" : "1201";
			else
				rail = "1211";
			break;
		}
		case 13: {
			rail = "1301";
			break;
		}
		case 16: {
			if (station < 1630)
				rail = dir ? "1601" : "1602";
			else if (station == 1630)
				rail = dir ? "1611" : "1602";
			else
				rail = dir ? "1611" : "1612";
			break;
		}

		default: {
			System.out.println("Cannot find record: Error Station ID" + stationID);
			break;
		}
		}
		BigDecimal dec = new BigDecimal(metroInter.get(day + time + rail) / 2.0).setScale(0, BigDecimal.ROUND_HALF_UP);
		int res = dec.toBigInteger().intValue();
		// System.out.println(day + time + rail);
		// System.out.println(metroInter.get(day + time + rail));
		// System.out.println("interval result:" + res);
		return res;
	}

	public static int getIntervalTime(String name1, String name2) {
		int res = 0;
		int i;
		for (i = 0; Character.isDigit(name1.charAt(i)); ++i)
			;
		String nameFirst = name1.substring(i);
		for (i = 0; Character.isDigit(name2.charAt(i)); ++i)
			;
		String nameSecond = name2.substring(i);
		if (intervalHash.get(nameFirst + "-" + nameSecond) != null) {
			res = intervalHash.get(nameFirst + "-" + nameSecond);
		}
		if (intervalHash.get(nameSecond + "-" + nameFirst) != null) {
			res = intervalHash.get(nameSecond + "-" + nameFirst);
		}
		if (res == 0) {
			System.out.println("###$$$" + nameFirst + "-" + nameSecond);
		}
		return res;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputPath, true);

		// get distributed file path
		Path trainInterval = new Path("hdfs://blade42:9000/home/gaozhu/StationInfo/TrainInterval.csv");
		Path stationOriginal = new Path("hdfs://blade42:9000/home/gaozhu/StationInfo/StationOriginal.csv");
		Path stationInterval = new Path("hdfs://blade42:9000/home/gaozhu/StationInfo/StationInterval.csv");
		Path shortestPath = new Path("hdfs://blade42:9000/home/gaozhu/StationInfo/ShortestPath.txt");
		DistributedCache.addCacheFile(trainInterval.toUri(), conf);
		DistributedCache.addCacheFile(stationOriginal.toUri(), conf);
		DistributedCache.addCacheFile(stationInterval.toUri(), conf);
		DistributedCache.addCacheFile(shortestPath.toUri(), conf);

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 0) {
			System.err.println("Usage: No arg needed!");
			System.exit(2);
		}
		Job job = new Job(conf, "Travel Time Expansion");

		job.setJarByClass(MRTravelTimeExpansion.class);
		job.setMapperClass(TravelTimeMapper.class);
		job.setReducerClass(IdentityReducer.class);

		job.setNumReduceTasks(18);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
