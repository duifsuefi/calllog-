package com.oracle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Main {
	
	public static void main(String[] args) throws IOException {
		 Configuration hadoopConf =  new Configuration();
		  Configuration hbaseConf = HBaseConfiguration.create(hadoopConf);
		  Connection conn = ConnectionFactory.createConnection(hbaseConf);
		  Admin admin = conn.getAdmin();
		  TableName tableName = TableName.valueOf("callInfo");
		  byte[][] splitKeys = {
		            Bytes.toBytes("01"),
		            Bytes.toBytes("02"),
		            Bytes.toBytes("03"),
		        };
		  if(admin.tableExists(tableName)) {
			  admin.disableTable(tableName);
			  admin.deleteTable(tableName);
		  }
		  if(!admin.tableExists(tableName)) {
			  HTableDescriptor tableDesc = new HTableDescriptor(tableName);
				//列族描述对象
				HColumnDescriptor family = new HColumnDescriptor("info");
				family.setMaxVersions(3);
				tableDesc.addFamily(family);
				admin.createTable(tableDesc,splitKeys);
		  }
		  HTable table = (HTable)conn.getTable(tableName);
		  table.setAutoFlushTo(false);
		Properties props = new Properties();
		// 定义kakfa 服务的地址，不需要将所有broker指定上 
		//props.put("bootstrap.servers", "node102:9092");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS102:9092");
		// 制定consumer group 
		props.put("group.id", "test1");
		// 是否自动确认offset 
		props.put("enable.auto.commit", "true");
		// 从何处消费数据(重要)（latest|earliest|none）
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// 自动确认offset的时间间隔 
		props.put("auto.commit.interval.ms", "1000");
		// key的序列化类
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// value的序列化类 
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// 定义consumer 
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);
		
		// 消费者订阅的topic, 可同时订阅多个 
		consumer.subscribe(Arrays.asList("three"));

		while (true) {
			// 读取数据，读取超时时间为100ms 
			ConsumerRecords<String, String> records = consumer.poll(100);
			List<Put> puts = new ArrayList<Put>();
			for (ConsumerRecord<String, String> record : records) {
				String str=record.value();
				//str=13162629996,赵浩歌,13042089546,刘墉,2020-06-19 03:04:59,1592550299448,5011
				String[] info=str.split(",");
				String rowKey=(info[4].substring(0,10).hashCode()+info[0].hashCode())%3+"_"+info[0]+" "+info[1]+" "+info[2]+" "+info[3]+" "+info[4]+" "+info[5]+" "+info[6]+" "+"1";
				if(rowKey.charAt(0)=='-') rowKey=rowKey.substring(1);
				rowKey="0"+rowKey;
				Put put= new Put(Bytes.toBytes(rowKey));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("callNum"), Bytes.toBytes(info[0]));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("callName"), Bytes.toBytes(info[1]));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("callToNum"), Bytes.toBytes(info[2]));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("callToName"), Bytes.toBytes(info[3]));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date"), Bytes.toBytes(info[4]));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dateStamp"), Bytes.toBytes(info[5]));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(info[6]));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("docall"), Bytes.toBytes("1"));
				puts.add(put);
				
				String rowKey2=(info[4].substring(0,10).hashCode()+info[2].hashCode())%3+"_"+info[2]+" "+info[3]+" "+info[0]+" "+info[1]+" "+info[4]+" "+info[5]+" "+info[6]+" "+"0";
				if(rowKey2.charAt(0)=='-') rowKey2=rowKey2.substring(1);
				rowKey2="0"+rowKey2;
				Put put2= new Put(Bytes.toBytes(rowKey2));
				put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("callNum"), Bytes.toBytes(info[2]));
				put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("callName"), Bytes.toBytes(info[3]));
				put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("callToNum"), Bytes.toBytes(info[0]));
				put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("callToName"), Bytes.toBytes(info[1]));
				put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date"), Bytes.toBytes(info[4]));
				put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dateStamp"), Bytes.toBytes(info[5]));
				put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(info[6]));
				put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("docall"), Bytes.toBytes("0"));
				puts.add(put2);
				
			}
			table.put(puts);

			if(puts.size()==6) {
				//达到一次小批次的处理条件后，手动提交推送缓冲区的请求
				table.flushCommits();
				//将容器中的已处理完毕的数据清空以防止重复处理
				puts.clear();
			}
			
		}
	}
}