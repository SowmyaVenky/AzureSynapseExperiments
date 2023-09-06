package com.usbank.synapse.beans;

public class Temperature {
    private double latitude;
    private double longitude;
    private double maxtemp;
    private double mintemp;
    @Override
    public String toString() {
        return "Temperature [latitude=" + latitude + ", longitude=" + longitude + ", maxtemp=" + maxtemp + ", mintemp="
                + mintemp + "]";
    }
    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }
    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
    public void setMaxtemp(double maxtemp) {
        this.maxtemp = maxtemp;
    }
    public void setMintemp(double mintemp) {
        this.mintemp = mintemp;
    }
    public double getLatitude() {
        return latitude;
    }
    public double getLongitude() {
        return longitude;
    }
    public double getMaxtemp() {
        return maxtemp;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(latitude);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(longitude);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxtemp);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(mintemp);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Temperature other = (Temperature) obj;
        if (Double.doubleToLongBits(latitude) != Double.doubleToLongBits(other.latitude))
            return false;
        if (Double.doubleToLongBits(longitude) != Double.doubleToLongBits(other.longitude))
            return false;
        if (Double.doubleToLongBits(maxtemp) != Double.doubleToLongBits(other.maxtemp))
            return false;
        if (Double.doubleToLongBits(mintemp) != Double.doubleToLongBits(other.mintemp))
            return false;
        return true;
    }
    public double getMintemp() {
        return mintemp;
    }
}
