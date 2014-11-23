package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import main.Environment;
import mapreduce.Configuration;
import mapreduce.Reducer;
import mapreduce.io.Context;
import mapreduce.io.Record;
import mapreduce.io.Records;
import mapreduce.io.TextOutputFormat;

public class reduceTest implements Runnable {
	public ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> loc;
	public Reducer  reducer;
	public String jobid;
	public String taskid;
	public String taskServiceName;
	public String jarpath;
	public int pNum;
	public reduceTest(String jid, String tid, ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> lc, String tName, String path, int num) 
	{
		jobid = jid;
		taskid = tid;
		taskServiceName = tName;
		loc = lc;
		jarpath = path;
		pNum=num;
	}
	public void run() {
		System.out.println("\n-------");
		Class<Reducer> reduceClass;
		String outpath = "";
		HashMap<String, List<String>> merge = new HashMap<String, List<String>>();
		try {
			reduceClass = load(this.jarpath);
			Constructor<Reducer> constructors = reduceClass
					.getConstructor();
			reducer = constructors.newInstance();
			if(reducer==null)
				System.out.println("??????");
			String input = "", line;
			String path=Environment.Dfs.DIRECTORY+"/";
			System.out.println(loc.size());
			for (String taskSerName : loc.keySet()) 
			{
				System.out.println("??????"+taskSerName);
			for (Integer maptaskid : loc.get(taskSerName).keySet()) 
			{
				String abpath=path+"/"+taskSerName+"/"+jobid+"/mapper/"+maptaskid+"_"+this.taskid;
				System.out.println("+++++"+abpath);
				File file = new File(abpath);

				BufferedReader reader = new BufferedReader(new FileReader(file));
				Vector<Record> target= new Vector<Record>();
					while ((line = reader.readLine()) != null) {
						String []tt=line.split("\t");
						Record inp =new Record(tt[0],tt[1]);
						target.add(inp);
					}
					for (Record a : target) {
						if (merge.containsKey(a.key))
							merge.get(a.key).add(a.value);
						else {
							List<String> hold = new ArrayList<String>();
							hold.add( a.value);
							merge.put( a.key, hold);
						}
					}
				
			}
		}
		Context ct = new Context(
				jobid, taskid, taskServiceName, false);

		for (String hold : merge.keySet()) {
			Records a = new Records(
					hold, merge.get(hold));
			reducer.reduce(a.getKey(), (ArrayList<String>)a.getValues(), ct);
		}
		outpath = Environment.Dfs.DIRECTORY + "/" + taskServiceName + "/"
				+ jobid + "/reducer/" + this.taskid;
		TextOutputFormat.writeTolocal(outpath, ct);
		} catch (Exception e) {
			e.printStackTrace();
			
		}	
	}

		public Class<Reducer> load(
				String jarFilePath) throws IOException, ClassNotFoundException {

			JarFile jarFile = new JarFile(jarFilePath);
			Enumeration<JarEntry> e = jarFile.entries();

			URL[] urls = { new URL("jar:file:" + jarFilePath + "!/") };
			ClassLoader cl = URLClassLoader.newInstance(urls);

			Class<Reducer> reducerClass = null;

			while (e.hasMoreElements()) {

				JarEntry je = e.nextElement();

				if (je.isDirectory() || !je.getName().endsWith(".class")) {
					continue;
				}

				String className = je.getName().substring(0,
						je.getName().length() - 6);
				className = className.replace('/', '.');
				System.out.println("find "+className);
				if (className.equals(test.WordCountReducer.class.getName())) {
					reducerClass = (Class<Reducer>) cl
							.loadClass(className);
				}
			}

			return reducerClass;

		}
	}
