package edu.soda.bus.process;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class BusRecordFilter {

	private static String inputPath = "/home/hellisk/Bus/201504.txt";
	private static String outputPath = "/home/hellisk/Bus/BusRecords";

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		File file = new File(inputPath);
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath), "UTF-8"));

		String line = "";
		while ((line = reader.readLine()) != null) {
			String[] record = line.split(",", 3);
			if (record[1].equals("10450") || record[1].equals("10221")) {
				writer.write(line+"\n");
			}
		}
		writer.flush();
		writer.close();
		reader.close();
	}

}
