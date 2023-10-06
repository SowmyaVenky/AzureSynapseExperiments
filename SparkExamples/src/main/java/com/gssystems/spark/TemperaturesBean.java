package com.gssystems.spark;

import com.google.gson.annotations.SerializedName;

public class TemperaturesBean {
    private double latitude;
    private double longitude;
    private double generationtime_ms;
    private int utc_offset_seconds;
    private String timezone;
    private String timezone_abbreviation;
    private double elevation;

    @SerializedName(value = "hourly_units")
    private HourlyUnitsBean hourlyUnits;

    @SerializedName(value = "hourly")
    private HourlyBean hourly;

    public double getLatitude() {
        return latitude;
    }
    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }
    public double getLongitude() {
        return longitude;
    }
    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
    public double getGenerationtime_ms() {
        return generationtime_ms;
    }
    public void setGenerationtime_ms(double generationtime_ms) {
        this.generationtime_ms = generationtime_ms;
    }
    public int getUtc_offset_seconds() {
        return utc_offset_seconds;
    }
    public void setUtc_offset_seconds(int utc_offset_seconds) {
        this.utc_offset_seconds = utc_offset_seconds;
    }
    public String getTimezone() {
        return timezone;
    }
    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }
    public String getTimezone_abbreviation() {
        return timezone_abbreviation;
    }
    public void setTimezone_abbreviation(String timezone_abbreviation) {
        this.timezone_abbreviation = timezone_abbreviation;
    }
    public double getElevation() {
        return elevation;
    }
    public void setElevation(double elevation) {
        this.elevation = elevation;
    }
    
    public HourlyUnitsBean getHourlyUnits() {
        return hourlyUnits;
    }
    public void setHourlyUnits(HourlyUnitsBean hourlyUnits) {
        this.hourlyUnits = hourlyUnits;
    }

    public HourlyBean getHourly() {
        return hourly;
    }
    public void setHourly(HourlyBean hourly) {
        this.hourly = hourly;
    }
}
