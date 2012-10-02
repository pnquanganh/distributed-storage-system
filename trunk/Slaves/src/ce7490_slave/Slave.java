package ce7490_slave;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import slave_client_interface.client_slave_interface;
import slave_master_interface.slave_master_interface;

public class Slave implements client_slave_interface {
	String address;
	Registry registry;
	private Info master_info;

	public Slave() throws RemoteException {
		Registry registry = LocateRegistry.getRegistry(master_info.getHost());
		boolean join_system = false;
		try {
			address = (InetAddress.getLocalHost()).toString();
		} catch (Exception e) {
			System.out.println("can't get inet address.");
		}
		int port = 3232;
		System.out.println("this address=" + address + ",port=" + port);
		try {
			registry = LocateRegistry.createRegistry(port);
			registry.rebind("SlaveServer", this);
			System.out.println("registry:" + registry.toString());
		} catch (RemoteException e) {
			System.out.println("remote exception" + e);
		}
		while (!join_system) {
			try {
				slave_master_interface smi = (slave_master_interface) registry
						.lookup(master_info.getName());
				Info info = new Info();
				info.setHost(address);
				info.setPort(port);
				info.setName("SlaveServer");
				join_system = smi.slave_join_dfs(info);
			} catch (NotBoundException e1) {
				e1.printStackTrace();
			} catch (RemoteException re) {
				re.printStackTrace();
			}
		}

	}

	@Override
	public byte[] read_data(String filename) throws RemoteException {
		File file = new File(filename);
		try {

			InputStream is = new FileInputStream(file);

			// Get the size of the file
			long length = file.length();

			// You cannot create an array using a long type.
			// It needs to be an int type.
			// Before converting to an int type, check
			// to ensure that file is not larger than Integer.MAX_VALUE.
			if (length > Integer.MAX_VALUE) {
				// File is too large
				// However this will not happen.

			}

			// Create the byte array to hold the data
			byte[] bytes = new byte[(int) length];

			// Read in the bytes
			int offset = 0;
			int numRead = 0;
			while (offset < bytes.length
					&& (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
				offset += numRead;
			}

			// Ensure all the bytes have been read in
			if (offset < bytes.length) {
				throw new IOException("Could not completely read file "
						+ file.getName());
			}

			// Close the input stream and return bytes
			is.close();
			return bytes;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void write_data(String filename, byte[] data) throws RemoteException {
		BufferedOutputStream bfoutput = null;
		try {
			FileOutputStream fileoutput = new FileOutputStream(new File(
					filename));
			bfoutput = new BufferedOutputStream(fileoutput);
			bfoutput.write(data);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		try {
			Slave slave = new Slave();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		while(true)
			;
	}

}
