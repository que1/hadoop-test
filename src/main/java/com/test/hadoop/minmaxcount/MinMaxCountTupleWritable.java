package com.test.hadoop.minmaxcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ProjectName: com.test.hadoop.minmaxcount
 * ClassName:   MinMaxCountTupleWriter
 * Copyright:
 * Company:     xunlei
 * @author:     queyiwen
 * @version:    1.0
 * @since:      jdk 1.7
 * Create at:   2019/4/26
 * Description:
 * <p>
 * <p>
 * Modification History:
 * Date       Author      Version      Description
 * -------------------------------------------------------------
 *
 *
 */
public class MinMaxCountTupleWritable implements Writable {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private Date min = new Date();
    private Date max = new Date();
    private Long count = 0L;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.min.getTime());
        out.writeLong(this.max.getTime());
        out.writeLong(this.count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.min = new Date(in.readLong());
        this.max = new Date(in.readLong());
        this.count = in.readLong();
    }

    public Date getMin() {
        return min;
    }

    public void setMin(Date min) {
        this.min = min;
    }

    public Date getMax() {
        return max;
    }

    public void setMax(Date max) {
        this.max = max;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return DATE_FORMAT.format(this.min) + '\t' + DATE_FORMAT.format(this.max) + '\t' + this.count;
    }
}
