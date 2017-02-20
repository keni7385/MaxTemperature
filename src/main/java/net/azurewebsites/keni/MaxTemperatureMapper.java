package net.azurewebsites.keni;

import java.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper 
  extends Mapper<LongWritble, Text, Text, IntWritable> {
    
    @Override
    public void map(LongWritble key, Text value, Context context)
      throws IOException, InterruptedException {
          String line = value.toString();
          String year = line.substring(15, 19);
          int airTemperature = Integer.parseInt(line.substring(87, 92));
          context.write(new Text(year), new IntWritable(airTemperature));
    }
}