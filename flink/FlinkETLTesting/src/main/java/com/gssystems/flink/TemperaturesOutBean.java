package com.gssystems.flink;

public class TemperaturesOutBean {
    private double latitude;
    private double longitude;
    private String time;
    private double temperature_2m;
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
    public String getTime() {
        return time;
    }
    public void setTime(String time) {
        this.time = time;
    }
    public Double getTemperature_2m() {
        return temperature_2m;
    }
    public void setTemperature_2m(Double temperature_2m) {
        this.temperature_2m = temperature_2m;
    }
    
}
