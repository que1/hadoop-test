package com.test.hadoop.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondaryReduce extends Reducer<IntPair, IntWritable, Text, IntWritable>{

    private final Text left = new Text();
    private static final Text SEPARATOR = new Text("====================================");

    public void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(SEPARATOR, null);
        left.set(Integer.toString(key.getFirst()));
        for (IntWritable val : values) {
            context.write(left, val);
        }
    }

}
