package dianxin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

public class Main {
	static HashMap<String, String> map=new HashMap<String,String>(){
		{
			put("13000009999", "许澄邈");
			 put("13000009998", "刘德泽");
			 put("13000009979", "程海超");
			 put("13000009966", "邓海阳");
			 put("13000009956", "邓海荣");
			 put("13120009999", "陈海逸");
			 put("13120009979", "宋海昌");
			 put("13120009977", "徐瀚钰");
			 put("13120009975", "徐瀚文");
			 put("13120009992", "陈涵亮");
			 put("13262529999", "程涵煦");
			 put("13262529997", "宋明宇");
			 put("13262529996", "徐涵衍");
			put("13262529995", "萬名博");
			put("13262529994", "万浩波");
			 put("13162629999", "荣浩博");
			put("13162629998", "陈浩初");
			 put("13162629997", "陈浩宕");
			put("13162629996", "赵浩歌");
			 put("13162629995", "周浩广");
			put("13000209999", "李公明");	
			 put("13000209998", "刘醒龙 ");
			 put("13000209991", "路佳瑄");
			put("13000209992", "刘继兴");
			 put("13000209993", "李建荣");
			 put("13042089547", "李轶男");
			put("13042089546", "刘墉");
			 put("13042089545", "贺茂峰");
			put("13042089544", "胡志平");
			put("13042089543", "黄育海");
			 put("13359525996", "狐眉儿");
			 put("13359525995", "韩浩月");
			 put("13359525994", "韩雨山");
			put("13359525993", "胡元骏");
			put("13359525992", "古清生");
			 put("18759525996", "恭小兵");
			 put("18759525991", "关凌");
			 put("18759525992", "高远");
			 put("18759525993", "郭妮");
			put("18759525994", "姜汉忠");
			put("15145287490", "郭灿金");
			 put("15145287491", "谷良");
			put("15145287492", "高建华");
			put("15145287493", "冯志丹");
			put("15145287494", "冯一萌");
			put("18745250909", "冯唐");
			put("18745250908", "傅光明");
			 put("18745250907", "方舟子");
			 put("18745250906", "戴鹏飞");
			put("18745250905", "崔岱远");
		}
	};

	static String[] callphonenum= {"13000009999","13000009998","13000009979","13000009966","13000009956",
							"13120009999","13120009979","13120009977","13120009975","13120009992",
							"13262529999","13262529997","13262529996","13262529995","13262529994",
							"13162629999","13162629998","13162629997","13162629996","13162629995",
							"13000209999","13000209998","13000209991","13000209992","13000209993",
							"13042089547","13042089546","13042089545","13042089544","13042089543",
							"13359525996","13359525995","13359525994","13359525993","13359525992",
							"18759525996","18759525991","18759525992","18759525993","18759525994",
							"15145287490","15145287491","15145287492","15145287493","15145287494",
							"18745250909","18745250908","18745250907","18745250906","18745250905"
	};
	
	public static String createCall() {
		String info="";
		 Random random=new Random();
		 int i=random.nextInt(50);
		 info+=callphonenum[i]+","+map.get(callphonenum[i])+",";
		 int t=random.nextInt(50);
		 while(t==i) {
			 t=random.nextInt(50);
		 }
			 info+=callphonenum[t]+","+map.get(callphonenum[t])+",";
		 
		return info;
	}
	private static long random(long begin,long end){

	    long rtn = begin + (long)(Math.random() * (end - begin));
	    //如果返回的是开始时间和结束时间，通过递归调用本函数查找随机值
	    if(rtn == begin || rtn == end){
	        return random(begin,end);
	    }
	    return rtn;
	 }
	
	public static String randomDate() throws ParseException {
		
		 SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		 
	        //定义开始时间
	        Date start = format.parse("2018-01-01 00:00:00");
	        //定义结束时间
	        Date endDate=new Date();
	        
	        String endtmp = format.format(endDate);
	        Date end=format.parse(endtmp);
	        if(start.getTime() >= end.getTime()){
	            return null;
	        }
	        long date = random(start.getTime(),end.getTime());
	        Date result=new Date(date);
	       
	        
	        return  format.format(result)+","+result.getTime()+",";
	
	}

	public static void main(String[] args) throws ParseException, InterruptedException, IOException {
		Random rand=new Random();
		File file=new File(args[0]);
		if(file.exists()) {
			file.delete();
		}
		file.createNewFile();
		FileWriter fw=new FileWriter(file);
		while(true) {
			Thread.currentThread().sleep(1000);
			BufferedWriter bWriter=new BufferedWriter(fw);
		// TODO Auto-generated method stub
		String info=createCall();
		 String date=randomDate();
		info+=date;
		int callTime=rand.nextInt(10000);
		info+=callTime;
		System.out.println(info);
		bWriter.append(info);
		bWriter.newLine();
		bWriter.flush();
	}

	}
}
