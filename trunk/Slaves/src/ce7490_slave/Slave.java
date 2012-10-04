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
import java.util.HashMap;
import java.util.Iterator;

import master_slave_interface.master_slave_interface;
import slave_master_interface.slave_master_interface;
import client_slave_interface.client_slave_interface;

public class Slave extends Thread implements client_slave_interface,
		master_slave_interface {
	Registry registry;
	private Info master_info;
	private Info slave_info;

	public Info get_Slave_Info() {
		return this.slave_info;
	}

	public Slave() throws RemoteException {
		Registry registry = LocateRegistry.getRegistry(master_info.getHost());
		boolean join_system = false;
		try {
			slave_info.setHost((InetAddress.getLocalHost()).toString());
		} catch (Exception e) {
			System.out.println("can't get inet address.");
		}
		
		int port = 3232;
		slave_info.setPort(port);
		System.out.println("this address=" + slave_info.getHost() + ",port=" + slave_info.getPort());
		try {
			registry = LocateRegistry.createRegistry(port);
			registry.rebind("SlaveServer", this);
			System.out.println("registry:" + registry.toString());
		} catch (RemoteException e) {
			System.out.println("remote exception" + e);
		}
		slave_info.setName(getName());
		while (!join_system) {
			try {
				slave_master_interface smi = (slave_master_interface) registry
						.lookup(master_info.getName());
				
				join_system = smi.slave_join_dfs(slave_info);
			} catch (NotBoundException e1) {
				e1.printStackTrace();
			} catch (RemoteException re) {
				re.printStackTrace();
			}
		}
		/*
		 * here we can do something more, when we add a new node, the master can
		 * ask the slaves to redistribute the files in the system. if we have
		 * time, we will add code here.
		 */
		// Todo: when we add a slave, we need to redistribute the files.

	}

	// public void redistribute_Files

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

	public static byte[] read_data_for_recovery(String filename,
			String slave_hostname, int slave_port, String slave_name)
			throws RemoteException {

		try {
			Registry registry = LocateRegistry.getRegistry(slave_hostname);
			client_slave_interface writer = (client_slave_interface) registry
					.lookup(slave_name);
			return writer.read_data(filename);

		} catch (RemoteException e) {
			System.err.println("Remote exception: " + e.toString());
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private byte[] xor(byte[] op1, byte[] op2) {
		byte[] result = new byte[op1.length];
		for (int i = 0; i < op1.length; i++) {
			result[i] = (byte) (op1[i] ^ op2[i]);
		}
		return result;
	}

	@Override
	public void recoverBlock(String filename, Hierachical_codes recoverd,
			HashMap<Hierachical_codes, Info> parts) throws RemoteException {

		Iterator<Hierachical_codes> keys = parts.keySet().iterator();
		Iterator<Info> values = parts.values().iterator();
		int size = parts.size();
		byte[][] recdata = new byte[size][];
		byte[] result = null;
		byte[] tmp = null;

		for (int i = 0; keys.hasNext() && values.hasNext(); i++) {
			Hierachical_codes hcode = keys.next();
			Info info = values.next();
			try {
				recdata[i] = read_data_for_recovery(filename + hcode,
						info.getHost(), info.getPort(), info.getName());
				if (i == 0)
					result = recdata[i];
				else
					result = xor(tmp, recdata[i]);
				tmp = result;

			} catch (RemoteException e) {
				throw e;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		this.write_data(filename + recoverd, result);

	}

	// public void recovery_data

	public static void main(String[] args) {
		try {
			Slave slave = new Slave();
			Thread heartbeat_thread = new Slave_HeartBeat(slave.get_Slave_Info());
			heartbeat_thread.start();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		// while (true)
		// ;
	}

}
