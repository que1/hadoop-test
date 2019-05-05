package com.test.hadoop.sort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

class GroupingComparator2 implements RawComparator<IntPair> {

    @Override
    public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
        return WritableComparator.compareBytes(bytes, i, Integer.SIZE / 8, bytes1, i2, Integer.SIZE / 8);
    }

    @Override
    public int compare(IntPair o1, IntPair o2) {
        int l = o1.getFirst();
        int r = o2.getFirst();
        return l == r ? 0 : (l < r ? -1 : 1);
    }
}
