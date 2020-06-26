package com.oracle.mr;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.oracle.bean.IntymacyBean;

public class Dfs2MysqlReducer extends Reducer<IntymacyBean, NullWritable, IntymacyBean, NullWritable>{
	int rank;
@Override
protected void reduce(IntymacyBean arg0, Iterable<NullWritable> arg1,
		Context arg2)
		throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	rank=1;		
	for(NullWritable value : arg1) {
				arg0.setRank(rank);
				arg2.write(arg0, value);
				rank++;
			}
}
}
