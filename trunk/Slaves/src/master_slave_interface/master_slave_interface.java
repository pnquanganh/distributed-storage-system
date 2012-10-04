package master_slave_interface;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;

import ce7490_slave.Hierachical_codes;
import ce7490_slave.Info;

/**
 * 
 * @author wangsibo
 */

public interface master_slave_interface extends Remote {
	void recoverBlock(String filename, Hierachical_codes recoverd,
			HashMap<Hierachical_codes, Info> parts) throws RemoteException;
}
