package slave_master_interface;

import java.rmi.RemoteException;

import ce7490_slave.Info;

public interface slave_master_interface {
	boolean slave_join_dfs(Info info) throws RemoteException;
	void slave_heartbeat(Info info) throws RemoteException;
}
