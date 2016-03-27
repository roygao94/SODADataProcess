package edu.soda.metro.io;

/**
 * Created by Roy Gao on 10/7/2015.
 */
public class WhenAndWhereAndPay implements Comparable<WhenAndWhereAndPay> {
	private String when;
	private String where;
	private double pay;

	public WhenAndWhereAndPay(String when, String where, double pay) {
		this.when = when;
		this.where = where;
		this.pay = pay;
	}

	public String getWhen() {
		return when;
	}

	public String getWhere() {
		return where;
	}

	public double getPay() {
		return pay;
	}

	@Override
	public String toString() {
		return when + "\t" + where + "\t" + pay;
	}
	
	@Override
	public int compareTo(WhenAndWhereAndPay o) {
		return this.getWhen().compareTo(o.getWhen());
	}
}
