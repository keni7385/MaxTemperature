package net.azurewebsites.keni;

import org.junit.*;
import java.IOException;
import org.apache.hadoop.io.*;
import org.apache.mrunit.mapreduce.MapDriver;

public class MaxTemperatureTest {

    @Test
    public void processValidRecord() throws IOException, InterruptedException {
        Text value = new Text("004301199099991950051518004+68750+023550FM-12+0382" +
                              "99999V0203201N00261220001CN9999999N9-00111+99999999999");

        new MapDriver<LongWritable, Text, Text, IntWritable>()
            .withMapper(new MaxTemperatureMapper())
            .withInput(new LongWritable(0), value)
            .withOutput(new Text("1950"), new IntWritable(-11))
            .runTest();
    }

    @Test
    public void ignoreMissingTemperatureRecord() throws IOException, InterruptedException {
        Text value = new Text("004301199099991950051518004+68750+023550FM-12+0382" +
                              "99999V0203201N00261220001CN9999999N9-99991+99999999999");
        
        new MapDriver<LongWritable, Text, Text, IntWritable>()
            .withMapper(new MaxTemperatureMapper())
            .withInput(new LongWritable(0), value)
            // asserts no output is produced
            .runTest();
    }
}