package com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TaggedRepairMapper extends Mapper<Object, Text, Text, Text> {
  public static final String REPAIR_COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";

  private Configuration configuration;

  @Override
  protected void map(Object key, Text row, Context context) throws IOException, InterruptedException {
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    configuration = context.getConfiguration();
  }

  protected String get(String key){
    return Preconditions.checkNotNull(configuration.get(key),
      "Expected %s to be present, but was not", key);
  }
}

