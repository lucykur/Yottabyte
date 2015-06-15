package com.thoughtworks.yottabyte.vehiclecount;

import org.apache.hadoop.io.Text;

/**
 * Created by lucykur on 15/06/15.
 */
public class Vehicle {
    private String[] vehicleDetails;

    public Vehicle(String vehicleDetails, String separator) {
        this.vehicleDetails = vehicleDetails.split(separator);
    }

    public String getType() {
        return vehicleDetails[0];
    }
}
