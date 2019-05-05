package com.test.hadoop.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class SecondaryMap extends Mapper<LongWritable, Text, IntPair, IntWritable> {

    private final IntPair intKey = new IntPair();
    private final IntWritable intValue = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        int left = 0;
        int right = 0;
        if (tokenizer.hasMoreTokens()) {
            left = Integer.parseInt(tokenizer.nextToken());
            if (tokenizer.hasMoreTokens()) {
                right = Integer.parseInt(tokenizer.nextToken());
            }
            intKey.set(left, right);
            intValue.set(right);
            context.write(intKey, intValue);
        }
    }

}
