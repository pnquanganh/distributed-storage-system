/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ce7490_client;

import hierachical_code.*;
import client_master_interface.*;
import client_slave_interface.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
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

import java.util.Map;

/**
 *
 * @author pham0071
 */
public class Client {

    public static String master_hostname;
    public static int master_port;
    public static String master_name;
    
    public static Writing_request_result get_writing_slaves(String filename,
            int filesize) throws Exception {
        try {
	    Registry registry = LocateRegistry.getRegistry(master_hostname);
	    client_master_interface writer = 
                    (client_master_interface) registry.lookup(master_name);
	    Writing_request_result list_slaves = 
                    writer.get_writing_slaves(filename, filesize);
            return list_slaves;
	} catch (RemoteException e) {
	    System.err.println("Remote exception: " + e.toString());
	    e.printStackTrace();
            throw e;
	} catch (Exception e) {
//	    System.err.println("Client exception: " + e.toString());
//	    e.printStackTrace();
            throw e;
	}
    }
    
    public static void write_data(String filename,
            byte[] data, String slave_hostname,
            int slave_port, String slave_name) throws Exception {
        try {
            Registry registry = LocateRegistry.getRegistry(slave_hostname);
            client_slave_interface writer =
                    (client_slave_interface) registry.lookup(slave_name);
            writer.write_data(filename, data);
            
        } catch (RemoteException e) {
            System.err.println("Remote exception: " + e.toString());
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
//	    System.err.println("Client exception: " + e.toString());
//	    e.printStackTrace();
            throw e;
        }
    }
    
    public static void write_to_slaves(String filename,
            Writing_request_result list_slaves,
            hierachical_code encoded_data) throws Exception{
        
        for (Map.Entry<Hierachical_codes, slave_info> entry : list_slaves.slaves.entrySet()) {
            Hierachical_codes code = entry.getKey();
            slave_info slave = entry.getValue();
            
            byte[] data_to_write;
            switch (code) {
                case O1:
                    data_to_write = encoded_data.getO1();
                    break;
                case O2:
                    data_to_write = encoded_data.getO2();
                    break;
                case O3:
                    data_to_write = encoded_data.getO3();
                    break;
                case O4:
                    data_to_write = encoded_data.getO4();
                    break;
                case O1O2:
                    data_to_write = encoded_data.getO1O2();
                    break;
                case O3O4:
                    data_to_write = encoded_data.getO3O4();
                    break;
                case O1O2O3O4:
                    data_to_write = encoded_data.getO1O2O3O4();
                    break;
                default:
                    throw new AssertionError();
            }
            
            write_data(filename, data_to_write, slave.getHost(), slave.getPort(), 
                    slave.getName());
        }
    }
    
    public static void write_operation(String filename) throws Exception {
        
        byte[] data = read_file(filename);
        
        Writing_request_result list_slaves = 
                get_writing_slaves(filename, data.length);
        
        hierachical_code encoder = new hierachical_code();
        encoder.encode(data);
        
        write_to_slaves(filename, list_slaves, encoder);
    }
    
    public static byte[] read_file(String filename){
        File file = new File(filename);
        byte[] data = new byte[(int)file.length()];
        
        try {
            InputStream input = null;
            try {
                int totalBytesRead = 0;
                input = new BufferedInputStream(new FileInputStream(file));
                while (totalBytesRead < data.length) {
                    int bytesRemaining = data.length - totalBytesRead;
                    //input.read() returns -1, 0, or more :
                    int bytesRead = input.read(data, totalBytesRead, bytesRemaining);
                    if (bytesRead > 0) {
                        totalBytesRead = totalBytesRead + bytesRead;
                    }
                }
                /*
                 the above style is a bit tricky: it places bytes into the 'data' array; 
                 'data' is an output parameter;
                 the while loop usually has a single iteration only.
                 */
                //log("Num bytes read: " + totalBytesRead);
                return data;
            } finally {
                //log("Closing input stream.");
                input.close();
            }
        } catch (FileNotFoundException ex) {
            //log("File not found.");
            return null;
        } catch (IOException ex) {
            //log(ex);
            return null;
        }
    }

    public static void write(byte[] aInput, String aOutputFileName) {
        //log("Writing binary file...");
        try {
            OutputStream output = null;
            try {
                output = new BufferedOutputStream(new FileOutputStream(aOutputFileName));
                output.write(aInput);
            } finally {
                output.close();
            }
        } catch (FileNotFoundException ex) {
            //log("File not found.");
        } catch (IOException ex) {
            //log(ex);
        }
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        String filename = "C:\\Users\\pham0071\\Downloads\\3 Constraints.pptx";
        
        byte[] data = read_file(filename);
        hierachical_code encoder = new hierachical_code();
        encoder.encode(data);
        
        hierachical_code decoder = new hierachical_code();
        decoder.setO2(encoder.getO2());
        decoder.setO3(encoder.getO3());
        decoder.setO4(encoder.getO4());
        //decoder.setO1O2(encoder.getO1O2());
        decoder.setO3O4(encoder.getO3O4());
        decoder.setO1O2O3O4(encoder.getO1O2O3O4());

        byte[] tmp = decoder.decode();
        byte[] original_data = null;
        if (tmp.length > data.length){
            original_data = new byte[data.length];
            System.arraycopy(tmp, 0, original_data, 0, data.length);
        }
        else{
            original_data = tmp;
        }

//        byte[] o2 = encoder.getO2();
//        byte[] new_o2 = decoder.getO2();
//        
//        System.out.println(new_o2.length);
//        for (int i = 0; i < new_o2.length; i++) {
//            if (o2[i] != new_o2[i]){
//                System.out.println(i + " " + o2[i] + " " + new_o2[i]);
//            }
//                
//            
//        }
        write(original_data, "test.pptx");
    }
}
