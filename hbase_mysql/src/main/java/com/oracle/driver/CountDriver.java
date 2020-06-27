package com.oracle.driver;

import java.io.IOException;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oracle.bean.IntymacyBean;
import com.oracle.mr.Dfs2MysqlMapper;
import com.oracle.mr.Dfs2MysqlReducer;
import com.oracle.mr.Hbase2DfsMapper;
import com.oracle.mr.Hbase2DfsReducer;
import com.oracle.output.MySQLOutputFormat;

public class CountDriver implements Tool {
	private Configuration conf;
	
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf = conf;
	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return this.conf;
	}
	

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(this.conf);
		
		// 配置 Job
		Scan scan = new Scan();
		// 设置 Mapper
		TableMapReduceUtil.initTableMapperJob(arg0[0], // 数据源的表名
				scan, // scan 扫描控制器
				Hbase2MysqlMapper.class, // 设置 Mapper 类
				Text.class, // 设置 Mapper 输出 key 类型
				Text.class, // 设置 Mapper 输出 value 值类型
				job// 设置给哪个 JOB
		);
		job.setReducerClass(HBase2MysqlReducer.class);
		
		job.setOutputFormatClass(MySQLOutputFormat.class);
		boolean result = job.waitForCompletion(true);
		if (!result) {
			throw new IOException("Job running with error");
		}
		return result?0:1;
	}
	
	public static class Hbase2MysqlMapper extends TableMapper<Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String rowKey = Bytes.toString(key.get());
			rowKey=rowKey.substring(3);
			//rowKey=13162629996 赵浩歌 13042089546 刘墉 2020-06-19 03:04:59 1592550299448 5011 1
			String[] info=rowKey.split(" ");
			if("1".equals(info[8])) {
				outValue.set(info[1]+" "+info[2]+" "+info[3]+" "+info[7]+" "+info[8]);
			}
			else
				outValue.set(info[1]+" "+info[2]+" "+info[3]+" "+"0"+" "+info[8]);
						
			//日维度
			
			outKey.set(info[0]+"_"+info[4]);
			context.write(outKey, outValue);
			//月维度
			if(info[4].length()>=4) {
			outKey.set(info[0]+"_"+info[4].substring(0,7));
			context.write(outKey, outValue);
			}
			//年维度
			if(info[4].length()>=4) {
			outKey.set(info[0]+"_"+info[4].substring(0,4));
			context.write(outKey, outValue);
			}
		}
	}
	
	public static class HBase2MysqlReducer extends Reducer<Text, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		int count,sum;
		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			count=0;
			sum=0;
			String name="";
			for(Text value : arg1) {
				 String[] info=value.toString().split(" ");
				sum+=Integer.valueOf(info[3]);
				if("1".equals(info[4])) {
					count++;
				}
				name=info[0];
			}
			outKey.set(arg0.toString());
			outValue.set(name+" "+sum+" "+count);
			arg2.write(outKey, outValue);
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Properties properties = System.getProperties();
	        properties.setProperty("HADOOP_USER_NAME", "bduser");
			System.setProperties(properties);
			Configuration conf =HBaseConfiguration.create(); 
			int result = ToolRunner.run(conf,new CountDriver(), args);
			System.exit(result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
