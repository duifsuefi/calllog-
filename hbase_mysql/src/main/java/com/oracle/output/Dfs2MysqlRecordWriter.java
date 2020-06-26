package com.oracle.output;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;
import com.oracle.bean.IntymacyBean;

public class Dfs2MysqlRecordWriter extends RecordWriter<IntymacyBean, NullWritable> {

	private Connection conn = null;
	private Statement stmt = null;
	private Hashtable<String, String> contactsMap=new Hashtable<String, String>();

	public Dfs2MysqlRecordWriter(TaskAttemptContext context) {
		// TODO Auto-generated constructor stub
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://CentOS103:3306/db_telecom?characterEncoding=utf8";
			String username = "root";
			String password = "root";
			conn = (Connection) DriverManager.getConnection(url, username, password);
			stmt = (Statement) conn.createStatement();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	@Override
	public void write(IntymacyBean key, NullWritable value) {
		// TODO Auto-generated method stub
		String callNum=key.getCallNum();
		String callToNum=key.getCallToNum();
		String querySql="";
		String callId="";
		String callToId="";
		ResultSet rs=null;
		if(callNum!=null&&callToNum!=null) {
		try {
		if(contactsMap.get(callNum)==null) {
			querySql = "SELECT id FROM tb_contacts WHERE telephone = "+callNum+";";
			 rs = stmt.executeQuery(querySql);
			rs.next();
			 callId = rs.getString("id");
			 contactsMap.put(callNum, callId);
		}
		else
			callId=contactsMap.get(callNum);
		if(contactsMap.get(callToNum)==null) {
			querySql = "SELECT id FROM tb_contacts WHERE telephone = "+callToNum+";";
			 rs = stmt.executeQuery(querySql);
			rs.next();
			 callToId = rs.getString("id");
			 contactsMap.put(callToNum, callToId);
		}
		else
			callToId=contactsMap.get(callToNum);
		String rank=key.getRank()+"";
		String count=key.getCount();
		String sum=key.getSum();
		if(callId!=""&&callToId!="") {
		String insertSql="INSERT INTO tb_intimacy (id,intimacy_rank,contact_id1,contact_id2,call_count,call_duration_count) VALUES (\r\n" + 
				"(select tmp.id from\r\n" + 
				"(select id from tb_intimacy where contact_id1="
				+callId
				+ " and contact_id2="
				+callToId
				+ ") as tmp),"
				+rank+","
				+callId+","
				+callToId+","
				+count+","
				+sum
				+")\r\n" + 
				"ON DUPLICATE KEY UPDATE intimacy_rank=VALUES(intimacy_rank), contact_id1=VALUES(contact_id1),contact_id2=VALUES(contact_id2),call_count=VALUES(call_count),call_duration_count=VALUES(call_duration_count);";
		stmt.execute(insertSql);
		}
		}catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		}
	}

	@Override
	public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(stmt!=null)
			try {
				stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		if(conn!=null)
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

}
