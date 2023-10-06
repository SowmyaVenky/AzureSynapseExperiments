package com.gssystems.spark;

import java.util.List;

public class HourlyBean {
    public List<String> getTime() {
        return time;
    }
    public void setTime(List<String> time) {
        this.time = time;
    }
    public List<Double> getTemperature_2m() {
        return temperature_2m;
    }
    public void setTemperature_2m(List<Double> temperature_2m) {
        this.temperature_2m = temperature_2m;
    }
    private List<String> time;
    private List<Double> temperature_2m;
}
