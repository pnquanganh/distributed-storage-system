package slave_master_interface;

import java.rmi.Remote;
import java.rmi.RemoteException;

import Info.Info;

public interface slave_master_interface extends Remote{
	boolean slave_join_dfs(Info info) throws RemoteException;
	void slave_heartbeat(Info info) throws RemoteException;
}
