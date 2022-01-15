package com.zhouhc.reduceJion;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneBean implements WritableComparable<PhoneBean> {
    private String id;
    private String pid;
    private int amount;
    private String pname;

    public PhoneBean() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public void set(String id, String pid, int amount, String pname) {
        this.id = id;
        this.pid = pid;
        this.amount = amount;
        this.pname = pname;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
    }

    @Override
    public int compareTo(PhoneBean o) {
        int compare = this.getPid().compareTo(o.getPid());
        if (compare == 0)
            return o.getPname().compareTo(this.getPname());
        return compare;
    }

    @Override
    public String toString() {
        return String.format("%s\t%s\t%s\t", this.id, this.pname, this.amount);
    }
}
