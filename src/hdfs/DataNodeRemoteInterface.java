package hdfs;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
public interface DataNodeRemoteInterface extends Remote{

	public boolean delete(String path, int ID);

	
}
