package mapreduce.io;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeMap;

public class TextOutputFormat {
	public static boolean writeTolocal(String filepath,Context ct)
	{
		try {
			BufferedWriter out=new BufferedWriter(new FileWriter(filepath));
			TreeMap<String,String> sorted = new TreeMap<String,String>();
			for(Record o :ct.ans)
			{
				sorted.put(o.getKey(), o.getValue());
					
			}
			for(String hold: sorted.keySet())
			{
				out.write(hold+"\t"+sorted.get(hold)+"\n");
			}
			out.close();
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("reduce task write ans to disk error");
			return false;
		}
		
	}
}
