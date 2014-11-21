package mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import mapreduce.io.Writable;

public class ReduceRunner implements Runnable{
	public Reducer<?, ?, ?, ?> reducer;
	public String jobid;
	public String taskid;
	public Configuration conf;
	public String taskServiceName;
	public String jarpath;
	public ConcurrentHashMap<String, ConcurrentHashMap<Integer, String> > loc;
	public ReduceRunner(String jid, String tid,ConcurrentHashMap<String, ConcurrentHashMap<Integer, String> > lc, Configuration cf,String tName,String path)
	{
		jobid=jid;
		taskid=tid;
		conf =cf;
		taskServiceName=tName;
		loc=lc;
		jarpath=path;
	}
	@Override
	public void run() {
		
		Class<Reducer<Writable,Writable,Writable,Writable>> reduceClass;
		
		try {
			reduceClass = load(this.jarpath);
			Constructor<Reducer<Writable,Writable,Writable,Writable>> constructors = reduceClass.getConstructor();
			reducer = constructors.newInstance();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 	
			
	}	
	public Class<Reducer<Writable, Writable, Writable, Writable>> load (String jarFilePath)
			throws IOException, ClassNotFoundException {
		
		JarFile jarFile = new JarFile(jarFilePath);
		Enumeration<JarEntry> e = jarFile.entries();
		
		URL[] urls = { new URL("jar:file:" + jarFilePath +"!/") };
		ClassLoader cl = URLClassLoader.newInstance(urls);
		
		Class<Reducer<Writable, Writable, Writable, Writable>> reducerClass = null;
		
		while (e.hasMoreElements()) {
            
			JarEntry je = e.nextElement();
            
			if(je.isDirectory() || !je.getName().endsWith(".class")){
                continue;
            }
            
            String className = je.getName().substring(0, je.getName().length() - 6);
            className = className.replace('/', '.');
            if (className.equals(conf.getReducerClass().getName())) {
            	reducerClass = (Class<Reducer<Writable, Writable, Writable, Writable>>) cl.loadClass(className);
            }
        }
		
		return reducerClass;
		
	}
	
	public void report(TaskInfo feedback) {
		
	}

	
}
