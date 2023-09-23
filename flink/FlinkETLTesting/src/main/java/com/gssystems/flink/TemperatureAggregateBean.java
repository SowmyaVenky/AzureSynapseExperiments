package com.gssystems.flink;

import java.io.Serializable;
import java.util.Objects;
class TemperatureAggregateBean implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3371046378221311516L;
	private double lat;
	private double lng;
	private String year;
	private String month;
	private double minTemp;
	private double maxTemp;

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLng() {
		return lng;
	}

	public void setLng(double lng) {
		this.lng = lng;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public double getMinTemp() {
		return minTemp;
	}

	public void setMinTemp(double minTemp) {
		this.minTemp = minTemp;
	}

	public double getMaxTemp() {
		return maxTemp;
	}

	public void setMaxTemp(double maxTemp) {
		this.maxTemp = maxTemp;
	}

	@Override
	public int hashCode() {
		return Objects.hash(lat, lng, maxTemp, minTemp, month, year);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TemperatureAggregateBean other = (TemperatureAggregateBean) obj;
		return Double.doubleToLongBits(lat) == Double.doubleToLongBits(other.lat)
				&& Double.doubleToLongBits(lng) == Double.doubleToLongBits(other.lng)
				&& Double.doubleToLongBits(maxTemp) == Double.doubleToLongBits(other.maxTemp)
				&& Double.doubleToLongBits(minTemp) == Double.doubleToLongBits(other.minTemp)
				&& Objects.equals(month, other.month) && Objects.equals(year, other.year);
	}

	@Override
	public String toString() {
		return "TemperatureAggregateBean [minTemp=" + minTemp + ", maxTemp=" + maxTemp + "]";
	}
	
	
}