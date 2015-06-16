package com.thoughtworks.yottabyte.vehiclecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        switch (key.toString().toUpperCase()){
            case "CAR" : return 0;
            case "TRUCK" : return 1;
            case "RICKSHAW" : return 2;
            case "BUS" : return 3;
            default: return 0;
        }
    }
}


