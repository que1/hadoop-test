package com.test.hadoop.minmaxcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * ProjectName: com.test.hadoop.minmaxcount
 * ClassName:   MinMaxCountJob
 * Copyright:
 * Company:     xunlei
 * @author:     queyiwen
 * @version:    1.0
 * @since:      jdk 1.7
 * Create at:   2019/4/29
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
public class MinMaxCountJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 2) {
            new IllegalArgumentException("Usage: <inpath> <outpath>");
            return;
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "minmaxcount-join");
        job.setJarByClass(MinMaxCountJob.class);
        // map settings
        job.setMapperClass(MinMaxCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MinMaxCountTupleWritable.class);
        // combiner settings
        // 因为combiner逻辑和reduce一样，所以此处可以直接使用MinMaxCountReducer.class
        job.setCombinerClass(MinMaxCountCombiner.class);
        // reduce settings
        job.setReducerClass(MinMaxCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxCountTupleWritable.class);

        //
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
