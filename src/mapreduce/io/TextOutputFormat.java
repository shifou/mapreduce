package mapreduce.io;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class TextOutputFormat {
	public static boolean writeTolocal(String filepath,Context<Writable,Writable> ct)
	{
		try {
			BufferedWriter out=new BufferedWriter(new FileWriter(filepath));
			for(Record o :ct.ans)
			{
					out.write(((Text)o.key).toString()+"\t"+((Text)o.value).toString()+"\n");
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
