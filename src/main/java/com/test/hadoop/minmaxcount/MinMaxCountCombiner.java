package com.test.hadoop.minmaxcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ProjectName: com.test.hadoop.minmaxcount
 * ClassName:   MinMaxCountCombiner
 * Copyright:
 * Company:     xunlei
 * @author:     queyiwen
 * @version:    1.0
 * @since:      jdk 1.7
 * Create at:   2020/2/24
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
public class MinMaxCountCombiner extends Reducer<Text, MinMaxCountTupleWritable, Text, MinMaxCountTupleWritable> {


    private MinMaxCountTupleWritable result = new MinMaxCountTupleWritable();

    @Override
    protected void reduce(Text key, Iterable<MinMaxCountTupleWritable> values, Context context) throws IOException, InterruptedException {
        //
        this.result.setMin(null);
        this.result.setMax(null);
        this.result.setCount(0L);
        long sum = 0L;

        for (MinMaxCountTupleWritable value : values) {
            if (this.result.getMin() == null || value.getMin().compareTo(this.result.getMin()) < 0) {
                this.result.setMin(value.getMin());
            }

            if (this.result.getMax() == null || value.getMax().compareTo(this.result.getMax()) > 0) {
                this.result.setMax(value.getMax());
            }

            sum += value.getCount();

        }

        this.result.setCount(sum);
        context.write(key, result);
    }

}
