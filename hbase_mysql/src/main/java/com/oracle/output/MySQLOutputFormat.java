package com.oracle.output;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

public class MySQLOutputFormat extends OutputFormat<Text,Text>{

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
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		RecordWriter<Text, Text> writer = new MyRecordWriter(arg0);
		return writer;
	}

	
}
