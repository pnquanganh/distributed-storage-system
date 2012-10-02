package ce7490_master;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import master_slave_interface.*;

import client_master_interface.*;

public class Master implements client_master_interface {
	public static HashMap<String, Reading_request_result> files;
	public static HashMap<Info, HashSet<String>> slaves;
	public static HashSet<Info> deadSlaves;

	public static Info getNextSlave() {
		return null;
	}

	public static void main(String[] args) {
		files = new HashMap<String, Reading_request_result>();
		slaves = new HashMap<Info, HashSet<String>>();
		deadSlaves = new HashSet<Info>();
	}

	public static void recovery() throws Exception {
		HashSet<String> damagedFiles = new HashSet<String>();

		for (Info dSlave : deadSlaves) {
			damagedFiles.addAll(slaves.get(dSlave));
			slaves.remove(dSlave);
		}

		for (String dFile : damagedFiles) {
			Reading_request_result fileInfo = files.get(dFile);
			HashMap<Hierachical_codes, Info> relatedSlaves = fileInfo.slaves;
			Vector<Hierachical_codes> missing = new Vector<Hierachical_codes>();

			for (Hierachical_codes i : Hierachical_codes.values()) {
				if (deadSlaves.contains(relatedSlaves.get(i))) {
					missing.add(i);
				}
			}

			HashMap<Hierachical_codes, HashSet<Hierachical_codes>> recovered = hierachical_code
					.recovery(missing);

			if (missing.size() > 0) {
				files.remove(dFile);
				// can not recover: data block lost
			} else {
				for (Hierachical_codes i : Hierachical_codes.values()) {
					if (recovered.get(i).size() > 1) {

						Info newSlave = getNextSlave();
						relatedSlaves.put(i, newSlave);

						HashSet<String> fileinSlave = slaves.get(newSlave);
						fileinSlave.add(dFile);

						HashMap<Hierachical_codes, Info> parts = new HashMap<Hierachical_codes, Info>();

						for (Hierachical_codes r : recovered.get(i))
							parts.put(r, relatedSlaves.get(r));

						recoveryToSlave(newSlave, dFile, i, parts);
					}
				}
			}

		}

		deadSlaves.clear();
	}

	private static void recoveryToSlave(Info newSlave, String dFile,
			Hierachical_codes i, HashMap<Hierachical_codes, Info> parts)
			throws Exception {
		try {
			Registry registry = LocateRegistry.getRegistry(newSlave.getHost());
			master_slave_interface recoverer = (master_slave_interface) registry.lookup(newSlave.getName());
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
		HashMap<Hierachical_codes, Info> fileslaves = new HashMap<Hierachical_codes, Info>();
		for (Hierachical_codes i : Hierachical_codes.values()) {
			Info newSlave = getNextSlave();
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
		return files.get(filename);
	}

}
