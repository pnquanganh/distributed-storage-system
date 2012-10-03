package client_server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import rmitest.Hello;

public class Server implements Hello {
	
    public Server() {}

    public String sayHello() {
	return "Hello, world!";
    }
	
    public static void main(String args[]) {
	
	try {
	    Server obj = new Server();
	    Hello stub = (Hello) UnicastRemoteObject.exportObject(obj, 0);

	    // Bind the remote object's stub in the registry
	    Registry registry = LocateRegistry.createRegistry(2031);
	    registry.bind("Hello", stub);

	    System.err.println("Server ready");
	} catch (Exception e) {
	    System.err.println("Server exception: " + e.toString());
	    e.printStackTrace();
	}
    }
}
