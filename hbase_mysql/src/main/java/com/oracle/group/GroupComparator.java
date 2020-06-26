package com.oracle.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.oracle.bean.IntymacyBean;

public class GroupComparator extends WritableComparator {

	public GroupComparator() {
		super(IntymacyBean.class, true);
		}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		return ((IntymacyBean)a).getCallNum().compareTo( ((IntymacyBean)b).getCallNum());
	}
}
