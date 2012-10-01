/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package client_master_interface;

/**
 *
 * @author pham0071
 */
public class slave_info {
    private String _host;
    private int _port;
    private String _name;

    /**
     * @return the _host
     */
    public String getHost() {
        return _host;
    }

    /**
     * @param host the _host to set
     */
    public void setHost(String host) {
        this._host = host;
    }

    /**
     * @return the _port
     */
    public int getPort() {
        return _port;
    }

    /**
     * @param port the _port to set
     */
    public void setPort(int port) {
        this._port = port;
    }

    /**
     * @return the _name
     */
    public String getName() {
        return _name;
    }

    /**
     * @param name the _name to set
     */
    public void setName(String name) {
        this._name = name;
    }
    
}
