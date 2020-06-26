package com.oracle.mr;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Hbase2DfsMapper extends TableMapper<Text, Text> {
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String rowKey = Bytes.toString(key.get());
			rowKey=rowKey.substring(3);
			String[] info=rowKey.split(" ");
			outKey.set(info[0]+"_"+info[2]);
			if(info[7].length()<=4)
			outValue.set(info[7]);
			else
				outValue.set("0");
			context.write(outKey, outValue);
		}
}

