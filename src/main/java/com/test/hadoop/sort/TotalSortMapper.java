package com.test.hadoop.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ProjectName: com.test.hadoop.sort
 * ClassName:   TotalSortMapper
 * Copyright:
 * Company:     xunlei
 * @author:     queyiwen
 * @version:    1.0
 * @since:      jdk 1.7
 * Create at:   2020/2/26
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
public class TotalSortMapper extends Mapper<LongWritable, Text, Text, NullWritable> {


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
         只按照key来排序，value字段不管，则可以把一行的两个数字分为key和value，
         使用
         String[] lineValues = value.toString().split(" ");
         context.write(new IntWritable(new Integer(lineValues[0])), new IntWritable(new Integer(lineValues[1])));
         */
        //一行两个字段，不切分，都作为key，然后通过Comparator接口实现里面去切分做大小比对
        context.write(value, NullWritable.get());



    }



}
