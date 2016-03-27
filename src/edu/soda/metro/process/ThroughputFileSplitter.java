package edu.soda.metro.process;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * Created by Roy Gao on 10/10/2015.
 */
public class ThroughputFileSplitter {

	private static String input = "/home/hellisk/Documents/-home-gaozhu-metroThroughput-part-r-00000";
	private static String output = "/home/hellisk/Documents/Metro-Table1/";

	public static void main(String[] args) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input), "UTF-8"));
		BufferedWriter writer = null;

		try {
			String line;
			String lastID = "", ID;
			while ((line = reader.readLine()) != null) {
				line = line.replaceAll("\t", ",");
				String[] split = line.split(",");
				ID = split[0];
				if (!lastID.equals(ID)) {
					if (!lastID.equals("")) {
//						assert writer != null;
						writer.flush();
						writer.close();
					}
					writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output + ID), "UTF-8"));
					lastID = ID;
				}
				writer.write(line + "\n");
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		reader.close();
	}
}
