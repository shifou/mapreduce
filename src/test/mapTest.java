package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import mapreduce.Mapper;
import mapreduce.io.Context;
import mapreduce.io.Record;
import mapreduce.io.RecordReader;
import mapreduce.io.Text;
import mapreduce.io.TextInputFormat;
import mapreduce.io.Writable;

public class mapTest implements Runnable {
	public String inpath;
	public String jarpath;
	public Mapper mapper;
	public String jobid;
	public String taskid;
	public int pNum;
	public String taskSerName;
	public mapTest(String path, String jpath, String jid, String tid,String taskName,int num) {
		jarpath = jpath;
		inpath = path;
		jobid=jid;
		taskid=tid;
		taskSerName=taskName;
		pNum=num;
	}

	public void run() { 
		System.out.println("????");
		File file = new File(inpath);
		String line, data = "";
		BufferedReader reader;
		Class<Mapper> mapClass;
		try {
			reader = new BufferedReader(new FileReader(file));
			mapClass = load(jarpath);
			mapper = (Mapper)mapClass.getConstructors()[0].newInstance();

			while ((line = reader.readLine()) != null) {
				data += (line + "\n");
			}
			Class<?> a = TextInputFormat.class;
			Class<RecordReader> inputFormatClass = (Class<RecordReader>) Class
					.forName(a.getName());
			Constructor<RecordReader> constuctor = inputFormatClass
					.getConstructor(String.class);
			RecordReader read = constuctor
					.newInstance(data);
			Context ct = new Context (
				jobid	, taskid, taskSerName, true);
			while (read.hasNext()) 
			{
				Record nextLine = read.nextKeyValue();
				mapper.map(nextLine.getKey().toString(), nextLine.getValue().toString(), ct);
			}
			
			ConcurrentHashMap<Integer, String> loc = ct.writeToDisk(pNum);

			System.out.printf("+++++++++");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Class<Mapper> load(
			String jarFilePath) throws IOException, ClassNotFoundException {

		JarFile jarFile = new JarFile(jarFilePath);
		Enumeration<JarEntry> e = jarFile.entries();

		URL[] urls = { new URL("jar:file:" + jarFilePath + "!/") };
		ClassLoader cl = URLClassLoader.newInstance(urls);

		Class<Mapper> mapperClass = null;

		while (e.hasMoreElements()) {

			JarEntry je = e.nextElement();

			if (je.isDirectory() || !je.getName().endsWith(".class")) {
				continue;
			}

			String className = je.getName().substring(0,
					je.getName().length() - 6);
			className = className.replace('/', '.');
			//System.out.println("classname "+className);
			Class<?> a= test.WordCountMap.class;
			if (className.equals(a.getName())) {
				mapperClass = (Class<Mapper>) cl
						.loadClass(className);
			}
		}

		return mapperClass;
	}
}
