package com.oracle.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntymacyBean implements WritableComparable<IntymacyBean>{

	String callNum,callToNum,count,sum;
	int rank;
	
	public int getRank() {
		return rank;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}

	public String getCallNum() {
		return callNum;
	}

	public void setCallNum(String callNum) {
		this.callNum = callNum;
	}

	public String getCallToNum() {
		return callToNum;
	}

	public void setCallToNum(String callToNum) {
		this.callToNum = callToNum;
	}

	public String getCount() {
		return count;
	}

	public void setCount(String count) {
		this.count = count;
	}

	public String getSum() {
		return sum;
	}

	public void setSum(String sum) {
		this.sum = sum;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return callNum+"\t"+callToNum+"\t"+count+"\t"+sum+"\t"+rank;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(callNum);
		out.writeUTF(callToNum);
		out.writeUTF(count);
		out.writeUTF(sum);
		out.writeInt(rank);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		callNum=in.readUTF();
		callToNum=in.readUTF();
		count=in.readUTF();
		sum=in.readUTF();
		rank=in.readInt();
	}

	public int compareTo(IntymacyBean o) {
		// TODO Auto-generated method stub
		int result=this.callNum.compareTo(o.getCallNum());
		if(result==0)  result=Integer.valueOf(o.sum)*Integer.valueOf(o.count)-Integer.valueOf(this.sum)*Integer.valueOf(this.count);
		return result;
	}

}
