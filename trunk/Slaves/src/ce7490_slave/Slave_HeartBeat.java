package ce7490_slave;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import slave_master_interface.slave_master_interface;

public class Slave_HeartBeat extends Thread {
	private Info master;
	private Info slave_info;

	public Slave_HeartBeat(Info slave_info) {
		this.slave_info = slave_info;
	}

	public void run() {
		while (true) {
			try {
				Registry registry = LocateRegistry
						.getRegistry(master.getHost());
				slave_master_interface writer = (slave_master_interface) registry
						.lookup(master.getName());
				writer.slave_heartbeat(slave_info);
				Thread.sleep(1000 * 120);
			} catch (RemoteException e) {
				System.err.println("Remote exception: " + e.toString());
			} catch (NotBoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
