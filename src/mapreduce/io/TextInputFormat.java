package mapreduce.io;

public class TextInputFormat extends RecordReader<IntWritable,Text>{

	private static final long serialVersionUID = -8587742415571171518L;
	public String []data;
	public int linenum;
	public int curLine;
	public TextInputFormat(String input)
	{
		curLine=0;
		data=input.split("\n");
		linenum=data.length;
	}
	public boolean hasNext() {
		return curLine<linenum;
	}

	public Record<IntWritable, Text> nextKeyValue() {
		Record<IntWritable,Text> ans= new Record<IntWritable, Text>();
		ans.key=new IntWritable(curLine);
		ans.value=new Text(data[curLine++]);
		return ans;
	}

}
