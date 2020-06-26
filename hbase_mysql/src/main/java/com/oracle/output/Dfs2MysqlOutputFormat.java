package com.oracle.output;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import com.oracle.bean.IntymacyBean;

public class Dfs2MysqlOutputFormat extends OutputFormat<IntymacyBean, NullWritable> {

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return;
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(null, arg0);
		}

	@Override
	public RecordWriter<IntymacyBean, NullWritable> getRecordWriter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		RecordWriter<IntymacyBean, NullWritable> writer=new Dfs2MysqlRecordWriter(arg0);
		return writer;
	}

}
