/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Info;



/**
 *
 * @author pham0071
 */
public class Info implements java.io.Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 */

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

	public boolean equals(Object obj) {
		if (obj instanceof Info) {
			Info info = (Info) obj;
			if (info.getHost().equals(this.getHost())
					&& info.getPort() == this.getPort()
					&& info.getName().equals(this.getName()))
				return true;
			return false;

		}
		return false;
	}

	public int hashCode() {
		String[] addrArray = this.getHost().split("\\.");

		long num = 0;

		for (int i = 0; i < addrArray.length; i++) {

			int power = 3 - i;

			num += ((Integer.parseInt(addrArray[i]) % 256 * Math
					.pow(256, power)));

		}

		return Long.valueOf(num).hashCode();
		// return 1;
		// return this.getHost().toString();
	}

}
