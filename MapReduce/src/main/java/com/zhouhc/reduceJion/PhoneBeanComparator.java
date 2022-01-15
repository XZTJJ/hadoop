package com.zhouhc.reduceJion;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PhoneBeanComparator extends WritableComparator {
    public PhoneBeanComparator() {
        super(PhoneBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PhoneBean Pa = (PhoneBean) a;
        PhoneBean Pb = (PhoneBean) b;
        return Pa.getPid().compareTo(Pb.getPid());
    }
}
