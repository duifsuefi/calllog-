package com.oracle.driver;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.oracle.bean.IntymacyBean;
import com.oracle.group.GroupComparator;
import com.oracle.mr.Dfs2MysqlMapper;
import com.oracle.mr.Dfs2MysqlReducer;
import com.oracle.output.Dfs2MysqlOutputFormat;

public class IntymacyDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "bduser");
		System.setProperties(properties);
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		// 6 指定本程序的 jar 包所在的本地路径
		job.setJarByClass(IntymacyDriver.class);
		// 2 指定本业务 job 要使用的 mapper/Reducer 业务类
		job.setMapperClass(Dfs2MysqlMapper.class);
		job.setReducerClass(Dfs2MysqlReducer.class);
		// 3 指定 mapper 输出数据的 kv 类型
		job.setMapOutputKeyClass(IntymacyBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setOutputFormatClass(Dfs2MysqlOutputFormat.class);
		// 5 指定 job 的输入原始文件所在目录
		Path input = new Path(args[0]);
//		Path output = new Path(args[1]);
//		FileSystem fs = FileSystem.get(configuration);
//		if (fs.exists(output)) {
//			fs.delete(output, true);
//		}
//		FileOutputFormat.setOutputPath(job, output);
		FileInputFormat.setInputPaths(job, input);
		
		// 7 将 job 中配置的相关参数，以及 job 所用的 java 类所在的 jar 包， 提交给
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
