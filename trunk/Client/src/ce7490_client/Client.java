/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ce7490_client;

import Info.Hierachical_codes;
import Info.Info;
import client_master_interface.*;
import client_slave_interface.*;
//import hierachical_code.*;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author pham0071
 */
public class Client {

	public static String master_hostname = "155.69.151.60";
	public static int master_port = 2055;
	public static String master_name = "Master";

	public static Writing_request_result get_writing_slaves(String filename,
			int filesize) throws Exception {
		try {
			Registry registry = LocateRegistry.getRegistry(master_hostname,
					master_port);
			client_master_interface writer = (client_master_interface) registry
					.lookup(master_name);
			Writing_request_result list_slaves = writer.get_writing_slaves(
					filename, filesize);
			return list_slaves;
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

	public static Reading_request_result get_reading_slaves(String filename)
			throws Exception {
		try {
			Registry registry = LocateRegistry.getRegistry(master_hostname,
					master_port);
			client_master_interface reader = (client_master_interface) registry
					.lookup(master_name);
			Reading_request_result list_slaves = reader
					.get_reading_slaves(filename);
			return list_slaves;
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

	public static void write_data(String filename, byte[] data,
			String slave_hostname, int slave_port, String slave_name)
			throws Exception {
		try {
			Registry registry = LocateRegistry.getRegistry(slave_hostname,
					slave_port);
			client_slave_interface writer = (client_slave_interface) registry
					.lookup(slave_name);
			writer.write_data(filename, data);

		} catch (RemoteException e) {
			System.err.println("Remote exception: " + e.toString());
			// e.printStackTrace();
			// throw e;
		} catch (Exception e) {
			// System.err.println("Client exception: " + e.toString());
			// e.printStackTrace();
			// throw e;
		}
	}

	public static byte[] read_data(String filename, String slave_hostname,
			int slave_port, String slave_name) throws Exception {
		try {
			Registry registry = LocateRegistry.getRegistry(slave_hostname,
					slave_port);
			client_slave_interface writer = (client_slave_interface) registry
					.lookup(slave_name);
			return writer.read_data(filename);

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

	public static void write_to_slaves(String filename,
			Writing_request_result list_slaves, hierachical_code encoded_data)
			throws Exception {

		for (Map.Entry<Hierachical_codes, Info> entry : list_slaves.slaves
				.entrySet()) {
			Hierachical_codes code = entry.getKey();
			Info slave = entry.getValue();

			byte[] data_to_write;
			String fname = filename;
			switch (code) {
			case O1:
				data_to_write = encoded_data.getO1();
				fname += ".O1";
				break;
			case O2:
				data_to_write = encoded_data.getO2();
				fname += ".O2";
				break;
			case O3:
				data_to_write = encoded_data.getO3();
				fname += ".O3";
				break;
			case O4:
				data_to_write = encoded_data.getO4();
				fname += ".O4";
				break;
			case O1O2:
				data_to_write = encoded_data.getO1O2();
				fname += ".O1O2";
				break;
			case O3O4:
				data_to_write = encoded_data.getO3O4();
				fname += ".O3O4";
				break;
			case O1O2O3O4:
				data_to_write = encoded_data.getO1O2O3O4();
				fname += ".O1O2O3O4";
				break;
			default:
				throw new AssertionError();
			}

			write_data(fname, data_to_write, slave.getHost(), slave.getPort(),
					slave.getName());
		}
	}

	public static byte[] read_from_slave(String filename,
			Reading_request_result list_slaves, Hierachical_codes code)
			throws Exception {
		try {
			Info slave = list_slaves.slaves.get(code);
			return read_data(filename, slave.getHost(), slave.getPort(),
					slave.getName());
		} catch (Exception e) {
			return null;
		}
	}

	public static void write_operation(String filename) throws Exception {

		byte[] data = read_file(filename);
		Writing_request_result list_slaves;
		try {
			list_slaves = get_writing_slaves(filename, data.length);
		} catch (Exception e) {
			return;
		}
		hierachical_code encoder = new hierachical_code();
		encoder.encode(data);

		System.out.println(encoder.getO1().length);
		write_to_slaves(filename, list_slaves, encoder);
	}

	public static byte[] read_operation(String filename) throws Exception {
		Reading_request_result metadata;
		try {
			metadata = get_reading_slaves(filename);
		} catch (Exception e) {
			return null;
		}

		byte[] o1 = null;
		byte[] o2 = null;
		byte[] o3 = null;
		byte[] o4 = null;
		byte[] o1o2 = null;
		byte[] o3o4 = null;
		byte[] o1o2o3o4 = null;

		HashMap<Hierachical_codes, Info> slaves = metadata.slaves;

		if ((!slaves.containsKey(Hierachical_codes.O1) && !slaves
				.containsKey(Hierachical_codes.O2))
				|| (!slaves.containsKey(Hierachical_codes.O3) && !slaves
						.containsKey(Hierachical_codes.O4))) {
			return null;
		}

		hierachical_code decoder = new hierachical_code();

		if (slaves.containsKey(Hierachical_codes.O1)) {
			o1 = read_from_slave(filename + ".O1", metadata,
					Hierachical_codes.O1);
		}
		else{
			System.out.println("Missing 01");
		}
		
		if (slaves.containsKey(Hierachical_codes.O2)) {
			o2 = read_from_slave(filename + ".O2", metadata,
					Hierachical_codes.O2);
		}
		else{
			System.out.println("Missing 02");
		}
		if (slaves.containsKey(Hierachical_codes.O3)) {
			o3 = read_from_slave(filename + ".O3", metadata,
					Hierachical_codes.O3);
		}
		else{
			System.out.println("Missing 03");
		}
		if (slaves.containsKey(Hierachical_codes.O4)) {
			o4 = read_from_slave(filename + ".O4", metadata,
					Hierachical_codes.O4);
		}
		else{
			System.out.println("Missing 04");
		}

		System.out.println(o1.length);
		
		if (o1 == null || o2 == null || o3 == null || o4 == null) {
			
			System.out.println("Missing");

			if (o1 == null || o2 == null) {
				if (slaves.containsKey(Hierachical_codes.O1O2)) {
					o1o2 = read_from_slave(filename + ".O1O2", metadata,
							Hierachical_codes.O1O2);
				} else {
					o3o4 = read_from_slave(filename + ".O3O4", metadata,
							Hierachical_codes.O3O4);
					o1o2o3o4 = read_from_slave(filename + ".O1O2O3O4",
							metadata, Hierachical_codes.O1O2O3O4);
				}
			}

			if (o3 == null || o4 == null) {
				if (slaves.containsKey(Hierachical_codes.O3O4) && o3o4 == null) {
					o3o4 = read_from_slave(filename + ".O3O4", metadata,
							Hierachical_codes.O3O4);
				} else {
					if (o1o2 == null) {
						o1o2 = read_from_slave(filename + ".O1O2", metadata,
								Hierachical_codes.O1O2);
					}
					if (o1o2o3o4 == null) {
						o1o2o3o4 = read_from_slave(filename + ".O1O2O3O4",
								metadata, Hierachical_codes.O1O2O3O4);
					}
				}
			}
		}

		decoder.setO1(o1);
		decoder.setO2(o2);
		decoder.setO3(o3);
		decoder.setO4(o4);
		decoder.setO1O2(o1o2);
		decoder.setO3O4(o3o4);
		decoder.setO1O2O3O4(o1o2o3o4);

		byte[] result_data = decoder.decode();
		if (result_data != null) {
			byte[] original_data;
			if (result_data.length > metadata.file_size) {
				original_data = new byte[metadata.file_size];
				System.arraycopy(result_data, 0, original_data, 0,
						metadata.file_size);
			} else {
				original_data = result_data;
			}
			return original_data;
		} else {
			return null;
		}

	}

	public static byte[] read_file(String filename) {
		File file = new File(filename);
		byte[] data = new byte[(int) file.length()];

		try {
			InputStream input = null;
			try {
				int totalBytesRead = 0;
				input = new BufferedInputStream(new FileInputStream(file));
				while (totalBytesRead < data.length) {
					int bytesRemaining = data.length - totalBytesRead;
					// input.read() returns -1, 0, or more :
					int bytesRead = input.read(data, totalBytesRead,
							bytesRemaining);
					if (bytesRead > 0) {
						totalBytesRead = totalBytesRead + bytesRead;
					}
				}
				/*
				 * the above style is a bit tricky: it places bytes into the
				 * 'data' array; 'data' is an output parameter; the while loop
				 * usually has a single iteration only.
				 */
				// log("Num bytes read: " + totalBytesRead);
				return data;
			} finally {
				// log("Closing input stream.");
				input.close();
			}
		} catch (FileNotFoundException ex) {
			// log("File not found.");
			return null;
		} catch (IOException ex) {
			// log(ex);
			return null;
		}
	}

	public static void write(byte[] aInput, String aOutputFileName) {
		// log("Writing binary file...");
		try {
			OutputStream output = new BufferedOutputStream(
					new FileOutputStream(aOutputFileName));
			output.write(aInput);
			output.flush();
			output.close();
		} catch (FileNotFoundException ex) {
			// log("File not found.");
		} catch (IOException ex) {
			// log(ex);
		}
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {
		// TODO code application logic here
		String filename = "gradschooltalk.pdf";

//		try{
//			System.out.println("Hello");
//		System.in.read();
//		}catch(Exception e){}
		
//		try {
//			write_operation(filename);
//		} catch (Exception e) {
//		}
//		System.out.println("Finished");

		try {
			byte[] res = read_operation(filename);
			write(res, "test.pdf");
		} catch (Exception e) {
		}

		// byte[] data = read_file(filename);
		// hierachical_code encoder = new hierachical_code();
		// encoder.encode(data);
		//
		// hierachical_code decoder = new hierachical_code();
		// decoder.setO2(encoder.getO2());
		// decoder.setO3(encoder.getO3());
		// decoder.setO4(encoder.getO4());
		// //decoder.setO1O2(encoder.getO1O2());
		// decoder.setO3O4(encoder.getO3O4());
		// decoder.setO1O2O3O4(encoder.getO1O2O3O4());
		//
		// byte[] tmp = decoder.decode();
		// byte[] original_data = null;
		// if (tmp.length > data.length){
		// original_data = new byte[data.length];
		// System.arraycopy(tmp, 0, original_data, 0, data.length);
		// }
		// else{
		// original_data = tmp;
		// }
		//
		// // byte[] o2 = encoder.getO2();
		// // byte[] new_o2 = decoder.getO2();
		// //
		// // System.out.println(new_o2.length);
		// // for (int i = 0; i < new_o2.length; i++) {
		// // if (o2[i] != new_o2[i]){
		// // System.out.println(i + " " + o2[i] + " " + new_o2[i]);
		// // }
		// //
		// //
		// // }
		// write(original_data, "test.pptx");
	}
}
