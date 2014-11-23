package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import mapreduce.io.Context;


public interface Reducer {
	
	public void reduce(String string,  ArrayList<String> values, Context ct ) throws IOException;
}
