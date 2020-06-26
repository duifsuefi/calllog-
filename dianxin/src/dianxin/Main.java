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
			put("13000009999", "�����");
			 put("13000009998", "������");
			 put("13000009979", "�̺���");
			 put("13000009966", "�˺���");
			 put("13000009956", "�˺���");
			 put("13120009999", "�º���");
			 put("13120009979", "�κ���");
			 put("13120009977", "�����");
			 put("13120009975", "�����");
			 put("13120009992", "�º���");
			 put("13262529999", "�̺���");
			 put("13262529997", "������");
			 put("13262529996", "�캭��");
			put("13262529995", "�f����");
			put("13262529994", "��Ʋ�");
			 put("13162629999", "�ٺƲ�");
			put("13162629998", "�ºƳ�");
			 put("13162629997", "�º��");
			put("13162629996", "�ԺƸ�");
			 put("13162629995", "�ܺƹ�");
			put("13000209999", "���");	
			 put("13000209998", "������ ");
			 put("13000209991", "·�Ѭu");
			put("13000209992", "������");
			 put("13000209993", "���");
			 put("13042089547", "������");
			put("13042089546", "��ܭ");
			 put("13042089545", "��ï��");
			put("13042089544", "��־ƽ");
			put("13042089543", "������");
			 put("13359525996", "��ü��");
			 put("13359525995", "������");
			 put("13359525994", "����ɽ");
			put("13359525993", "��Ԫ��");
			put("13359525992", "������");
			 put("18759525996", "��С��");
			 put("18759525991", "����");
			 put("18759525992", "��Զ");
			 put("18759525993", "����");
			put("18759525994", "������");
			put("15145287490", "���ӽ�");
			 put("15145287491", "����");
			put("15145287492", "�߽���");
			put("15145287493", "��־��");
			put("15145287494", "��һ��");
			put("18745250909", "����");
			put("18745250908", "������");
			 put("18745250907", "������");
			 put("18745250906", "������");
			put("18745250905", "���Զ");
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
	    //������ص��ǿ�ʼʱ��ͽ���ʱ�䣬ͨ���ݹ���ñ������������ֵ
	    if(rtn == begin || rtn == end){
	        return random(begin,end);
	    }
	    return rtn;
	 }
	
	public static String randomDate() throws ParseException {
		
		 SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		 
	        //���忪ʼʱ��
	        Date start = format.parse("2018-01-01 00:00:00");
	        //�������ʱ��
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
