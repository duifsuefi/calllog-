package com.oracle.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Hbase2DfsReducer extends Reducer<Text, Text, Text, Text>{
	Text outKey = new Text();
	Text outValue = new Text();
	int count,sum;
	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		count=0;
		sum=0;
		for(Text value : arg1) {
			sum+=Integer.valueOf(value.toString());
			count++;
		}
		outKey.set(arg0.toString());
		outValue.set(count+"_"+sum);
		arg2.write(outKey, outValue);
	}

}
