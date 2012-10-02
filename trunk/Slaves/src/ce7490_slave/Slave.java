package ce7490_slave;

import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import slave_client_interface.client_slave_interface;

public class Slave implements client_slave_interface {
	String address;
	Registry registry;
	private Master_Info master_info;
	
	public Slave() throws RemoteException {
		try {
			address = (InetAddress.getLocalHost()).toString();
		} catch (Exception e) {
			System.out.println("can't get inet address.");
		}
		int port = 3232;
		System.out.println("this address=" + address + ",port=" + port);
		try {
			registry = LocateRegistry.createRegistry(port);
			registry.rebind("rmiServer", this);
			System.out.println("registry:"+registry.toString());
		} catch (RemoteException e) {
			System.out.println("remote exception" + e);
		}
	}

	@Override
	public byte[] read_data(String filename) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void write_data(String filename, byte[] data) throws RemoteException {
		// TODO Auto-generated method stub

	}

	public static void main(String[] args) {
		try {
			Slave slave = new Slave();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}
