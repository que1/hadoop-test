package com.test.hadoop.sort;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {

    private int first;
    private int second;

    public void set(int left, int right) {
        this.first = left;
        this.second = right;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public int compareTo(IntPair o) {
        if (this.first != o.first) {
            return this.first < o.first ? -1 : 1;
        } else if (this.second != o.second) {
            return this.second < o.second ? -1 : 1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.first);
        out.writeInt(this.second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readInt();
        this.second = in.readInt();
    }

    @Override
    public int hashCode() {
        return this.first * 157 + this.second;
    }

    @Override
    public boolean equals(Object right) {
        if (right == null)
            return false;
        if (this == right)
            return true;
        if (right instanceof IntPair) {
            IntPair r = (IntPair) right;
            return r.first == this.first && r.second == this.second;
        } else {
            return false;
        }
    }

}
