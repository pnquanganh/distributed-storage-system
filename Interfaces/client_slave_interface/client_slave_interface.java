/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package client_slave_interface;

import java.rmi.Remote;
import java.rmi.RemoteException;
/**
 *
 * @author pham0071
 */
public interface client_slave_interface extends Remote {
    byte[] read_data(String filename) throws RemoteException;
    void write_data(String filename, byte[] data) throws RemoteException;
}
