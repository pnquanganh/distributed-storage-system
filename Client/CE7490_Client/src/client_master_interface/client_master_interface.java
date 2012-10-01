/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package client_master_interface;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 *
 * @author pham0071
 */


public interface client_master_interface extends Remote {
    Writing_request_result get_writing_slaves(String filename, int filesize) throws RemoteException;
    Reading_request_result get_reading_slaves(String filename) throws RemoteException;    
}
