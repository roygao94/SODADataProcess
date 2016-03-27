package edu.soda.bus.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

public class BusInputRecord implements WritableComparable<BusInputRecord> {

	private String busID;
	private String station = "";
	private int direction = 0;
	private Date arrivingTime;
	private boolean status;
	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public BusInputRecord() {
		this.busID = "";
		this.station = "";
		this.arrivingTime = null;
	}

	public BusInputRecord(String bID, String station, int dir, Date aTime, boolean sta) {
		this.busID = bID;
		this.station = station;
		this.direction = dir;
		this.arrivingTime = aTime;
		this.status = sta;
	}

	// public void addNextStation(BusStation busStation) {
	// this.nextStation.add(busStation);
	// }
	//
	// public void addNextStation(String nextStationName) {
	// this.nextStation.add(new BusStation(nextStationName));
	// }

	public String getBusID() {
		return this.busID;
	}

	public String getCurrentStation() {
		return this.station;
	}

	public int getDirection() {
		return this.direction;
	}

	public Date getArrivingTime() {
		return this.arrivingTime;
	}

	public boolean getStatus() {
		return this.status;
	}

	public int compareTo(BusInputRecord busRec) {
		return this.getArrivingTime().compareTo(busRec.getArrivingTime());
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(direction);
		out.writeBoolean(status);
		out.writeUTF(busID + "," + station + "," + format.format(arrivingTime));
	}

	public void readFields(DataInput in) throws IOException {
		direction = in.readInt();
		status = in.readBoolean();
		String[] rec = in.readUTF().split(",");
		busID = rec[0];
		station = rec[1];
		try {
			arrivingTime = format.parse(rec[2]);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
