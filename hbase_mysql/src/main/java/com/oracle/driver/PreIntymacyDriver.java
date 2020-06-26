package com.oracle.driver;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oracle.mr.Hbase2DfsMapper;
import com.oracle.mr.Hbase2DfsReducer;

public class PreIntymacyDriver implements Tool{
	private Configuration conf;
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

	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf = conf;
	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return this.conf;
		}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
Job job = Job.getInstance(this.conf);
		
		// 配置 Job
		Scan scan = new Scan();
		// 设置 Mapper
		TableMapReduceUtil.initTableMapperJob(args[0], // 数据源的表名
				scan, // scan 扫描控制器
				Hbase2DfsMapper.class, // 设置 Mapper 类
				Text.class, // 设置 Mapper 输出 key 类型
				Text.class, // 设置 Mapper 输出 value 值类型
				job// 设置给哪个 JOB
		);
		job.setReducerClass(Hbase2DfsReducer.class);
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);
		boolean result = job.waitForCompletion(true);
		if (!result) {
			throw new IOException("Job running with error");
		}
		return result?0:1;
		
	}

}
