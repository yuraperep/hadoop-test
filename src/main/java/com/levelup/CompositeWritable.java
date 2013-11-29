package com.levelup;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: Yuriy Perepelytsya
 * Date: 11/28/13 : 4:56 PM
 * Powered by IDEA
 */
public class CompositeWritable implements WritableComparable<CompositeWritable> {

    private String first;
    private String second;

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readUTF();
        second = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeUTF(second);
    }

    public CompositeWritable(String first, String second) {
        this.first = first;
        this.second = second;
    }

    public CompositeWritable() {
    }

    @Override
    public int compareTo(CompositeWritable o) {
        int compareFirst = this.first.compareTo(o.first);
        int compareSecond = this.second.compareTo(o.second);
        if (compareFirst != 0)
            return compareFirst;
        else
            return compareSecond;
    }
}
