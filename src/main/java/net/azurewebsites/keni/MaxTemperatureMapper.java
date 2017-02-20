package net.azurewebsites.keni;

import java.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper 
  extends Mapper<LongWritble, Text, Text, IntWritable> {
    
    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    public void map(LongWritble key, Text value, Context context)
      throws IOException, InterruptedException {
          parser.parse(value);
          if(parser.isValidTemperature()) {
            context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
          }
    }
}