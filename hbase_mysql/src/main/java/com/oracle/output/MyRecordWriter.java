package com.oracle.output;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

public class MyRecordWriter extends RecordWriter<Text, Text> {
	private Connection conn = null;
	private Statement stmt = null;
	private Hashtable<String, String> contactsMap=new Hashtable<String, String>();
	private Hashtable<String, String> dataMap=new Hashtable<String, String>();
	public MyRecordWriter(TaskAttemptContext context) {
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
	public void write(Text key, Text value) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] keyString=key.toString().split("_");
		System.out.println(key.toString());
		String[] valueString=value.toString().split(" ");
		if(keyString.length>1) {
			String[] dimensions=keyString[1].split("-");
			String year="-1",month="-1",day="-1";
			if(dimensions.length>2)
				day=dimensions[2];
			if(dimensions.length>1)
				month=dimensions[1];
			if(dimensions.length>0)
				year=dimensions[0];
			System.out.println(year+" "+month+" "+day);
			if(year.length()<5&&day!=null&&month!=null) {
			try {
				String querySql="";
				ResultSet rs=null;
				String id_contact="";
				if(contactsMap.get(keyString[0])==null) {
				String insertToContacts="INSERT INTO tb_contacts (id,telephone,name) VALUES(\r\n" + 
						"(select tmp.id from \r\n" + 
						"(select id from tb_contacts where telephone='"
						+ keyString[0]
						+ "') as tmp),'"
						+keyString[0]
						+ "','"
						+valueString[0]
						+ "') \r\n" + 
						"ON DUPLICATE KEY UPDATE telephone=VALUES(telephone),name=VALUES(name)";
				stmt.execute(insertToContacts);				
				 querySql = "SELECT id FROM tb_contacts WHERE telephone = "+keyString[0]+";";
				 rs = stmt.executeQuery(querySql);
				rs.next();
				 id_contact = rs.getString("id");
				 contactsMap.put(keyString[0], id_contact);
				}
				else {
					id_contact=contactsMap.get(keyString[0]);
				}
				String id_date_dimension="";
				if(dataMap.get(year+month+day)==null) {
				String insertToDate="INSERT INTO tb_dimension_date (id,year,month,day) VALUES(\r\n" + 
						"(select tmp.id from \r\n" + 
						"(select id from tb_dimension_date where year="
						+year
						+ " and month="
						+month
						+ " and day="
						+day
						+ ") as tmp)\r\n" + 
						","
						+year
						+ ","
						+month
						+ ","
						+day
						+ ") \r\n" + 
						"ON DUPLICATE KEY UPDATE year=VALUES(year),month=VALUES(month),day=VALUES(day)";
						stmt.execute(insertToDate);
						
						querySql = "SELECT id FROM tb_dimension_date WHERE "
								+ "year = "+year+" AND month = "+month
								+" AND day = "+day+";";
						rs = stmt.executeQuery(querySql);
						rs.next();
						 id_date_dimension = rs.getString("id");
						 dataMap.put(year+month+day, id_date_dimension);
						}
						else {
							id_date_dimension=dataMap.get(year+month+day);
						}
						String id_date_contact = id_date_dimension+"_"+id_contact;
						String insertSql="INSERT INTO tb_call (id_date_contact,id_date_dimension,id_contact,call_sum,call_duration_sum) VALUES(\r\n" + 
								"'"
								+id_date_contact
								+"',"
								+id_date_dimension
								+ ","
								+id_contact
								+ ","
								+valueString[2]
								+ ","
								+valueString[1]
								+ ") \r\n" + 
								"ON DUPLICATE KEY UPDATE id_date_contact=VALUES(id_date_contact),id_date_dimension=VALUES(id_date_dimension),id_contact=VALUES(id_contact),call_sum=VALUES(call_sum),call_duration_sum=VALUES(call_duration_sum)";
								stmt.execute(insertSql);
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			
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
