package edu.soda.taxi.process;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * Created by Roy Gao on 10/10/2015.
 */
public class TaxiFileSplitter {

	private static String input = "/home/hellisk/Documents/-home-gaozhu-taxi-statistics-2-part-r-00000";
	private static String output = "/home/hellisk/Documents/Taxi/";

	public static void main(String[] args) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input), "UTF-8"));
		BufferedWriter writer = null;

		try {
			String line;
			String lastPosition = "", position;
			while ((line = reader.readLine()) != null) {
				String[] split = line.split(",", 3);
				position = split[0]+"_"+split[1];
				if (!lastPosition.equals(position)) {
					if (!lastPosition.equals("")) {
//						assert writer != null;
						writer.flush();
						writer.close();
					}
					writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output + position), "UTF-8"));
					lastPosition = position;
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
