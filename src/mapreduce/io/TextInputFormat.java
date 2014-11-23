package mapreduce.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TextInputFormat extends RecordReader {

	private static final long serialVersionUID = -8587742415571171518L;
	public String[] data;
	public int linenum;
	public int curLine;

	public TextInputFormat(String input) {
		curLine = 0;
		data = input.split("\n");
		linenum = data.length;
		System.out.println("ffffff-----"+input);
	}
	/*
	public boolean readRecords() {
		String input = "", line;
		File file = new File(path);
		if (file.exists() == false)
			return false;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			while ((line = reader.readLine()) != null) {
				input += (line + "\n");
			}
			data = input.split("\n");
			linenum = data.length;
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	*/
	public boolean hasNext() {
		return curLine < linenum;
	}

	public Record nextKeyValue() {
		Record  ans = new Record();
		ans.key = new LongWritable(curLine);
		ans.value = new Text(data[curLine++]);
		return ans;
	}

}
