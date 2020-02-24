package com.test.hadoop.average;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ProjectName: com.test.hadoop.average
 * ClassName:   AverageReducer
 * Copyright:
 * Company:     xunlei
 * @author:     queyiwen
 * @version:    1.0
 * @since:      jdk 1.7
 * Create at:   2019/5/6
 * Description:
 * <p>
 * <p>
 * Modification History:
 * Date       Author      Version      Description
 * -------------------------------------------------------------
 *
 *
 */
public class AverageReducer extends Reducer<IntWritable, CountAverageTupleWritable, IntWritable, CountAverageTupleWritable> {

    //
    private CountAverageTupleWritable result = new CountAverageTupleWritable();

    @Override
    public void reduce(IntWritable key, Iterable<CountAverageTupleWritable> values, Context context) throws IOException, InterruptedException {
        //
        long sum = 0;
        long count = 0;

        for (CountAverageTupleWritable value : values) {
            //
            sum += (value.getCount() * value.getAverage());
            count += value.getCount();
        }

        result.setCount(count);
        result.setAverage(sum / count);

        context.write(key, result);
    }

}
