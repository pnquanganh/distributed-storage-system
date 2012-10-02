package ce7490_slave;

public class Master_Info {
	/**
	 * @return the master_hostname
	 */
	public String getMaster_hostname() {
		return master_hostname;
	}
	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}
	/**
	 * @return the master_rmi_name
	 */
	public String getMaster_rmi_name() {
		return master_rmi_name;
	}
	private String master_hostname;
	private int port;
	private String master_rmi_name;
}
