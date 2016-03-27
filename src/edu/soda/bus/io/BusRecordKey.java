package edu.soda.bus.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class BusRecordKey implements WritableComparable<BusRecordKey> {

	private String busID;
	private String previousStation = "";
	private String currentStation = "";
	private int previousStationNum = 0;
	private int direction = 0;

	public BusRecordKey() {
		busID = "";
	}

	public BusRecordKey(String bID, String pStation, String cStation, int pStationNum, int dir) {
		this.busID = bID;
		this.previousStation = pStation;
		this.currentStation = cStation;
		this.previousStationNum = pStationNum;
		this.direction = dir;
	}

	public String getBusID() {
		return busID;
	}

	public String getPreviousStation() {
		return previousStation;
	}

	public String getCurrentStation() {
		return currentStation;
	}

	public int getPreviousStationNum() {
		return previousStationNum;
	}

	public int getDirection() {
		return direction;
	}

	public int compareTo(BusRecordKey busRec) {
		if (this.getBusID().equals(busRec.getBusID())) {
			if (this.getDirection() == busRec.getDirection()) {
				return Integer.valueOf(this.getPreviousStationNum()).compareTo(
						Integer.valueOf(busRec.getPreviousStationNum()));
			} else
				return Integer.valueOf(this.getDirection()).compareTo(Integer.valueOf(busRec.getDirection()));
		} else
			return this.getBusID().compareTo(busRec.getBusID());
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(direction);
		out.writeInt(previousStationNum);
		out.writeUTF(busID + "," + previousStation + "," + currentStation);
	}

	public void readFields(DataInput in) throws IOException {
		direction = in.readInt();
		previousStationNum = in.readInt();
		String[] rec = in.readUTF().split(",");
		busID = rec[0];
		previousStation = rec[1];
		currentStation = rec[2];
	}
}
