package ce7490_master;

import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Vector;

import Info.Hierachical_codes;
import Info.Info;

import slave_master_interface.*;
import master_slave_interface.*;
import client_master_interface.*;

public class Master extends UnicastRemoteObject implements
		client_master_interface, slave_master_interface {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static HashMap<String, Reading_request_result> files;
	public static HashMap<Info, HashSet<String>> slaves;
	public static HashMap<Info, Long> timeStamps;
	public static HashSet<Info> deadSlaves;
	public static Random rand;

	public static Info getNextSlave(HashSet<Info> excludedSlaves, Random rand) {

		HashSet<Info> remainSlaves = new HashSet<Info>(slaves.keySet());
		remainSlaves.removeAll(excludedSlaves);

		return (Info) remainSlaves.toArray()[rand.nextInt(remainSlaves.size())];
	}

	public Master() throws RemoteException {
		try {
			Registry registry = LocateRegistry.createRegistry(2055);
			registry.rebind("Master", this);

			System.err.println("Master ready");
			System.err.println(InetAddress.getLocalHost().getHostAddress()
					.toString()
					+ ":2055");
		} catch (Exception e) {
			System.err.println("Master exception: " + e.toString());
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws RemoteException {
		files = new HashMap<String, Reading_request_result>();
		slaves = new HashMap<Info, HashSet<String>>();
		deadSlaves = new HashSet<Info>();
		rand = new Random(System.currentTimeMillis());
		timeStamps = new HashMap<Info, Long>();

		try {
			new Master();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		(new TimeThread()).start();
		// Info newinf = new Info();
	}

	public static void recovery() throws Exception {
		HashSet<String> damagedFiles = new HashSet<String>();

		for (Info dSlave : deadSlaves) {
			damagedFiles.addAll(slaves.get(dSlave));
			slaves.remove(dSlave);
			timeStamps.remove(dSlave);
		}

		for (String dFile : damagedFiles) {
			Reading_request_result fileInfo = files.get(dFile);
			HashMap<Hierachical_codes, Info> relatedSlaves = fileInfo.slaves;
			Vector<Hierachical_codes> missing = new Vector<Hierachical_codes>();
			HashSet<Info> excludedSlaves = new HashSet<Info>();

			for (Hierachical_codes i : Hierachical_codes.values()) {
				if (deadSlaves.contains(relatedSlaves.get(i))) {
					missing.add(i);
				} else {
					excludedSlaves.add(relatedSlaves.get(i));
				}
			}

			HashMap<Hierachical_codes, HashSet<Hierachical_codes>> recovered = hierachical
					.recovery(missing);

			if (missing.size() > 0) {
				
				System.out.println(dFile+" has been lost due to slave failure!");
				files.remove(dFile);
				// can not recover: data block lost
			} else {
				for (Hierachical_codes i : Hierachical_codes.values()) {
					if (recovered.get(i).size() > 1) {

						if (excludedSlaves.size() == slaves.size())
							excludedSlaves.clear();
						Info newSlave = getNextSlave(excludedSlaves, rand);
						excludedSlaves.add(newSlave);

						relatedSlaves.put(i, newSlave);

						HashSet<String> fileinSlave = slaves.get(newSlave);
						fileinSlave.add(dFile);

						HashMap<Hierachical_codes, Info> parts = new HashMap<Hierachical_codes, Info>();

						for (Hierachical_codes r : recovered.get(i))
							parts.put(r, relatedSlaves.get(r));

						recoveryToSlave(newSlave, dFile, i, parts);
					}
				}
				
				System.out.println(dFile+" has been recovered!");
			}

		}

		deadSlaves.clear();
	}

	private static void recoveryToSlave(Info newSlave, String dFile,
			Hierachical_codes i, HashMap<Hierachical_codes, Info> parts)
			throws Exception {
		
		System.out.println("Recovering ***********");
		System.out.println(newSlave.getHost()+":"+newSlave.getPort()+" "+i+" ");
		
		for (Hierachical_codes ri : parts.keySet()){
			System.out.println(parts.get(ri).getHost()+":"+parts.get(ri).getPort()+" "+ri+" ");
		}
		System.out.println("*********************");
		
		
		try {
			Registry registry = LocateRegistry.getRegistry(newSlave.getHost(),
					newSlave.getPort());
			master_slave_interface recoverer = (master_slave_interface) registry
					.lookup(newSlave.getName());
			recoverer.recoverBlock(dFile, i, parts);

		} catch (RemoteException e) {
			System.err.println("Remote exception: " + e.toString());
			// e.printStackTrace();
			throw e;
		} catch (Exception e) {
			// System.err.println("Client exception: " + e.toString());
			// e.printStackTrace();
			throw e;
		}
	}

	@Override
	public Writing_request_result get_writing_slaves(String filename,
			int filesize) throws RemoteException {
		// TODO Auto-generated method stub

		System.out.println("Write file: " + filename);

		HashMap<Hierachical_codes, Info> fileslaves = new HashMap<Hierachical_codes, Info>();
		HashSet<Info> excludedSlaves = new HashSet<Info>();

		for (Hierachical_codes i : Hierachical_codes.values()) {

			if (excludedSlaves.size() == slaves.size())
				excludedSlaves.clear();
			Info newSlave = getNextSlave(excludedSlaves, rand);
			excludedSlaves.add(newSlave);

			fileslaves.put(i, newSlave);
			HashSet<String> fileinSlave = slaves.get(newSlave);
			fileinSlave.add(filename);
		}

		Reading_request_result newFile = new Reading_request_result();
		newFile.file_size = filesize;
		newFile.slaves = fileslaves;
		files.put(filename, newFile);

		Writing_request_result writeResponse = new Writing_request_result();
		writeResponse.slaves = fileslaves;
		return writeResponse;
	}

	@Override
	public Reading_request_result get_reading_slaves(String filename)
			throws RemoteException {
		// TODO Auto-generated method stub

		System.out.println("Read file: " + filename);

		return files.get(filename);
	}

	@Override
	public boolean slave_join_dfs(Info info) throws RemoteException {
		// TODO Auto-generated method stub

		slaves.put(info, new HashSet<String>());
		System.out.println(info.getHost()+":"+ info.getPort() + " joins in!");

		timeStamps.put(info, (Long) (System.currentTimeMillis() / 1000));

		// System.out.println(string);

		return true;
	}

	public static void check() throws Exception {
		// TODO Auto-generated method stub

		Long currentTime = System.currentTimeMillis() / 1000;
		System.out.println("Checking timestamps....");

		for (Info i : slaves.keySet()) {
			if (currentTime - timeStamps.get(i) > 10) {
				deadSlaves.add(i);
				System.out.println("Dead Slave:" + i.getHost());
			}
		}

		if (!deadSlaves.isEmpty()) {
			recovery();
		}
	}

	@Override
	public void slave_heartbeat(Info info) throws RemoteException {
		// TODO Auto-generated method stub
		 //System.out.println(slaves.size());
		
		if (slaves.containsKey(info)) {
			timeStamps.put(info, (Long) (System.currentTimeMillis() / 1000));
			System.out.println(info.getHost()+":" +info.getPort()+ " heartbeating...");
		}
	}

}
