package com.test.hadoop.average;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * ProjectName: com.test.hadoop.average
 * ClassName:   CountAverageTupleWritable
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
 *
 */
public class CountAverageTupleWritable implements Writable {

    private long count;
    private long average;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.count);
        out.writeLong(this.average);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.count = in.readLong();
        this.average = in.readLong();
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getAverage() {
        return average;
    }

    public void setAverage(long average) {
        this.average = average;
    }

}
