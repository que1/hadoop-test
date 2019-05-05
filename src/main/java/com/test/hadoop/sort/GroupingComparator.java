package com.test.hadoop.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {

    protected GroupingComparator() {
        super(IntPair.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        IntPair intPair1 = (IntPair) w1;
        IntPair intPair2 = (IntPair) w2;
        int l = intPair1.getFirst();
        int r = intPair2.getFirst();
        return  l == r ? 0 : (l < r ? -1 : 1);
    }

}
