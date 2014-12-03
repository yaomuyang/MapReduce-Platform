package commons;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;

/*
 * In this Communication module, there are a bunch of static communication functions that we can freely use
 */
public class Communication {
	
	//cache the dataoutputstreams and datainputstream for a certain socket connection
	private static HashMap<Socket, DataOutputStream> out = new HashMap<Socket, DataOutputStream>();
	private static HashMap<Socket, DataInputStream> in = new HashMap<Socket, DataInputStream>();
	
	/*
	 * Return the DataOutputStream based on the socket instance
	 * If we have already cached the dataOutputstream for this socket, we will simply use the previous one
	 * Else, we create a new one
	 */
	private static DataOutputStream getOut(Socket s) throws Exception{
		//System.out.println("Communication, output: "+out.size());
		if(out.containsKey(s)){
			return out.get(s);
		}else{
			DataOutputStream oos = new DataOutputStream(s.getOutputStream());
			out.put(s, oos);
			return oos;
		}
	}
	
	public static void deleteSocket(Socket s){
		out.remove(s);
		in.remove(s);
	}
	
	/*
	 * Return the DataInputStream based on the socket instance
	 * If we have already cached the dataInputstream for this socket, we will simply use the previous one
	 * Else, we create a new one
	 */
	private static DataInputStream getIn(Socket s) throws Exception{
		if(in.containsKey(s)){
			return in.get(s);
		}else{
			DataInputStream ois = new DataInputStream(s.getInputStream());
			in.put(s, ois);
			return ois;
		}
	}
	
	/*
	 * sendString()
	 * Basic communication function, send a string to the other side of the socket stream
	 */
    public static void sendString(String string, Socket s) throws Exception{
    	Communication.sendBytes(s, string.getBytes());
    }
    
    /*
	 * sendObject()
	 * Basic communication function, send a serializable object to the other side of the socket stream
	 */
    public static void sendObject(Object object, Socket s) throws Exception{
    	byte[] bytes = toBytes(object);
    	sendBytes(s, bytes);
    }
    
    /*
	 * receiveString()
	 * Basic communication function, receive a String from the other side of the socket
	 */
    public static String receiveString(Socket s) throws Exception{
    	byte[] bytes = Communication.receiveBytes(s);
    	return new String(bytes);
    }
    
    /*
	 * receiveObject()
	 * Basic Communication function, receive a Serialized object from the other side of the socket
	 */
    public static Object receiveObject(Socket s) throws Exception{
		byte[] bytes = receiveBytes(s);
    	return toObject(bytes);
    }
    
	/*
	 * sendBytes()
	 * Basic communication function, send a byte string to the other side, this is typically used when seending objects or files
	 */
	public static void sendBytes(Socket s, byte[] bytes) throws Exception{
		DataOutputStream dos = Communication.getOut(s);
    	dos.writeInt(bytes.length);
    	dos.flush();
    	dos.write(bytes);
    	dos.flush();
	}
	
	/*
	 * receiveBytes()
	 * Basic communication function, receive a byte string from the other side, this is typically used when seending objects or files
	 */
	public static byte[] receiveBytes(Socket s) throws Exception{
		DataInputStream dis = Communication.getIn(s);
    	int size = dis.readInt();
    	byte[] bytes = new byte[size];
    	dis.readFully(bytes);
    	return bytes;
	}
	
    /*
  	 * private function toBytes()
  	 * used in sendObject, pack the object into byte[]
  	 */
	private static byte[] toBytes(Object object) throws Exception{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(object);
		return baos.toByteArray();
	}

	/*
  	 * private function toObject()
  	 * used in recieveObject, unpack the byte[] into an object
  	 */
	private static Object toObject(byte[] bytes) throws Exception{
		Object object = null;
		object = new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
		return object;
	}
}
