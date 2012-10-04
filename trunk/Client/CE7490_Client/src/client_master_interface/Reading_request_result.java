/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package client_master_interface;

import java.util.HashMap;

public class Reading_request_result implements java.io.Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public HashMap<Hierachical_codes, Info> slaves;
    public int file_size;
}
