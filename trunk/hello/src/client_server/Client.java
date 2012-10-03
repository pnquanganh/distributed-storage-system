package client_server;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import rmitest.Hello;

public class Client {

    private Client() {}

    public static void main(String[] args) {

	//String host = (args.length < 1) ? null : args[0];
	String host = "rmi://155.69.151.60:2024/Hello";
    	//String host = "rmi://155.69.151.60:2020/Hello";
	try {
	    Registry registry = LocateRegistry.getRegistry(host);
	    System.out.println("Hi");
//	    Hello stub = (Hello) registry.lookup("Hello");
	    Hello stub = (Hello) Naming.lookup(host);
    String response = stub.sayHello();
	    System.out.println("response: " + response);
	} catch (Exception e) {
	    System.err.println("Client exception: " + e.toString());
	    e.printStackTrace();
	}
    }
}

