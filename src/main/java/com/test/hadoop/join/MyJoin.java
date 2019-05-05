package com.test.hadoop.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Vector;

/**
 * ProjectName: com.test.hadoop.join
 * ClassName:   MyJoin
 * Copyright:
 * Company:     xunlei
 * @author:     queyiwen
 * @version:    1.0
 * @since:      jdk 1.7
 * Create at:   2019/4/24
 * Description:
 * <p>
 * <p>
 * Modification History:
 * Date       Author      Version      Description
 * -------------------------------------------------------------
 *
 *
 *
 */
public class MyJoin {

    public static Logger logger = LoggerFactory.getLogger(MyJoin.class);

    public static class MyMap extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            LongWritable longWritableKey = new LongWritable();
            Text valueText = new Text();

            String line = value.toString();
            if (line == null || line.length() == 0) {
                return;
            }

            String[] values = line.split(",");
            if (values.length == 2) {
                String id = values[0];
                String name = values[1];
                System.out.println("#employee," + id + "," + name);
                logger.info("#employee," + id + "," + name);

                longWritableKey.set(Long.valueOf(id));
                valueText.set("#employee," + name);
                context.write(longWritableKey, valueText);
            } else if (values.length == 3) {
                String id = values[0];
                String produceId = values[1];
                String saleNum = values[2];
                System.out.println("#sale," + id + "," + produceId + "," + saleNum);
                logger.info("#sale," + id + "," + produceId + "," + saleNum);

                longWritableKey.set(Long.valueOf(id));
                valueText.set("#sale," + produceId + "," + saleNum);
                context.write(longWritableKey, valueText);
            }
        }

    }


    public static class MyReduce extends Reducer<LongWritable, Text, LongWritable, Text>{


        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Vector<String> vectorEmployee = new Vector<String>();
            Vector<String> vectorSales = new Vector<String>();

            Text valueText = new Text();

            for (Text text : values) {
                String value = text.toString();
                if (value.startsWith("#employee")) {
                    String[] results = value.split(",");
                    vectorEmployee.add(results[1]);
                } else if (value.startsWith("#sale")) {
                    String[] results = value.split(",");
                    vectorSales.add(results[1] + " " + results[2]);
                }
            }


            for (String employee : vectorEmployee) {
                for (String sale : vectorSales) {
                    valueText.set(employee + "----" + sale);
                    context.write(key, valueText);
                }
            }

        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            new IllegalArgumentException("Usage: <inpath> <outpath>");
            return;
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "my-join");
        job.setJarByClass(MyJoin.class);
        // map settings
        job.setMapperClass(MyMap.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        // reduce settings
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        //
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
