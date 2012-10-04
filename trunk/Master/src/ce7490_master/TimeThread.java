package ce7490_master;

public class TimeThread extends Thread {
	public void run() {
		while (true) {
			try {
				Thread.sleep(12*1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				Master.check();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
