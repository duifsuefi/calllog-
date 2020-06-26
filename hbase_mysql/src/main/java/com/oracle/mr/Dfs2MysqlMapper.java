package com.oracle.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oracle.bean.IntymacyBean;

public class Dfs2MysqlMapper extends Mapper<LongWritable, Text, IntymacyBean, NullWritable>{
		IntymacyBean outkey=new IntymacyBean();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntymacyBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String[] info=value.toString().split("\t");
			if(info.length>1&&info[0].length()>12) {
			String[] nums=info[0].split("_");
			String[] total=info[1].split("_");
			outkey.setCallNum(nums[0]);
			outkey.setCallToNum(nums[1]);
			outkey.setCount(total[0]);
			outkey.setSum(total[1]);
			context.write(outkey, NullWritable.get());
			
			}
		}
}
