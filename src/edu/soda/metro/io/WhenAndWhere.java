package edu.soda.metro.io;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Roy Gao on 10/9/2015.
 */
public class WhenAndWhere {

	private SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd,HH:mm:ss");
	private Date time;
	private String ID;
	private String name;
	private int direction;

	public WhenAndWhere(Date time, String ID, String name, int direction) {
		this.time = time;
		this.ID = ID;
		this.name = name;
		this.direction = direction;
	}

	public WhenAndWhere(String str) {
		String[] split = str.split(",");
		if (split.length == 5) {
			try {
				time = dateFormat.parse(split[0] + "," + split[1]);
				ID = split[2];
				name = split[3];
				direction = Integer.parseInt(split[4]);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void setTime(Date date) {
		this.time = date;
	}

	public void setID(String ID) {
		this.ID = ID;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setDirection(int direction) {
		this.direction = direction;
	}

	public Date getTime() {
		return time;
	}

	public String getTimeStr() {
		return dateFormat.format(time);
	}

	public String getID() {
		return ID;
	}

	public String getName() {
		return name;
	}

	public int getDirection() {
		return direction;
	}

	public String toString() {
		return dateFormat.format(time) + "," + ID + "," + name + ","
				+ direction;
	}
}
