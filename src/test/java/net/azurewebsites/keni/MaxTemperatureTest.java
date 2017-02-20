package net.azurewebsites.keni;

import org.junit.*;
import java.IOException;
import org.apache.hadoop.io.*;
import org.apache.mrunit.mapreduce.MapDriver;

public class MaxTemperatureTest {

    @Test
    public void processValidRecord() throws IOException, InterruptedException {
        Text value = new Text("...");

        new MapDriver<LongWritable, Text, Text, IntWritable>()
            .withMapper(new MaxTemperatureMapper())
            .withInput(new LongWritable(0), value)
            .withOutput(new Text("1950"), new IntWritable(-11))
            .runTest();
    }
}