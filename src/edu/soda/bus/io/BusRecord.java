package edu.soda.bus.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

public class BusRecord implements WritableComparable<BusRecord> {

	private String busID;
	private String previousStation = "";
	private String currentStation = "";
	private int previousStationNum = 0;
	private int currentStationNum = 0;
	private int direction = 0;
	private Date leavingTime;
	private long duration = 0;
	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public BusRecord() {
		busID = "";
	}

	public BusRecord(String bID, String pStation, String cStation, int pStationNum, int cStationNum, int dir,
			Date lTime, long dur) {
		this.busID = bID;
		this.previousStation = pStation;
		this.currentStation = cStation;
		this.previousStationNum = pStationNum;
		this.currentStationNum = cStationNum;
		this.direction = dir;
		this.leavingTime = lTime;
		this.duration = dur;
	}

	// public void addNextStation(BusStation busStation) {
	// this.nextStation.add(busStation);
	// }
	//
	// public void addNextStation(String nextStationName) {
	// this.nextStation.add(new BusStation(nextStationName));
	// }

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

	public int getCurrentStationNum() {
		return currentStationNum;
	}

	public int getDirection() {
		return direction;
	}

	public Date getLeavingTime() {
		return leavingTime;
	}

	public long getDuration() {
		return duration;
	}

	public int compareTo(BusRecord busRec) {
		return this.getPreviousStationNum() == busRec.getPreviousStationNum() ? this.getLeavingTime().compareTo(
				busRec.getLeavingTime()) : Integer.valueOf(this.getPreviousStationNum()).compareTo(
				Integer.valueOf(busRec.getPreviousStationNum()));
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(direction);
		out.writeLong(duration);
		out.writeInt(previousStationNum);
		out.writeInt(currentStationNum);
		out.writeUTF(busID + "," + previousStation + "," + currentStation + "," + format.format(leavingTime));
	}

	public void readFields(DataInput in) throws IOException {
		direction = in.readInt();
		duration = in.readLong();
		previousStationNum = in.readInt();
		currentStationNum = in.readInt();
		String[] rec = in.readUTF().split(",");
		busID = rec[0];
		previousStation = rec[1];
		currentStation = rec[2];
		try {
			leavingTime = format.parse(rec[3]);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
