package edu.soda.bus.process;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * Created by Roy Gao on 10/10/2015.
 */
public class BusFileSplitter {

	private static String input = "/home/hellisk/Documents/-home-gaozhu-bus-statistic-part-r-00000";
	private static String output = "/home/hellisk/Documents/Bus/";

	public static void main(String[] args) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input), "UTF-8"));
		BufferedWriter writer = null;

		try {
			String line;
			String lastBusLine= "", BusLine;
			while ((line = reader.readLine()) != null) {
				String[] split = line.split("\t");
				BusLine = split[0];
				if (!lastBusLine.equals(BusLine)) {
					if (!lastBusLine.equals("")) {
//						assert writer != null;
						writer.flush();
						writer.close();
					}
					writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output + BusLine), "UTF-8"));
					lastBusLine = BusLine;
				}
				writer.write(line.replaceAll("\t", ",") + "\n");
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		reader.close();
	}
}
