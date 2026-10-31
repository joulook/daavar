package impl;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;

// Integer socket implementation (trusted, contracts defined in UDPSocketHelper.javaspec)
public class UDPSocketHelper {
    public static boolean sendInt(DatagramSocket outSocket, int toSend) throws IOException /*@ ensures true; @*/
    {
        byte[] bytes = ByteBuffer.allocate(4).putInt(toSend).array();
        DatagramPacket packet = new DatagramPacket(bytes, 4);
        outSocket.send(packet);
        return true;
    }

    public static Integer receiveInt(DatagramSocket recSocket) throws IOException
    {
        byte[] bytes = ByteBuffer.allocate(4).array();
        DatagramPacket packet = new DatagramPacket(bytes, 4);
        recSocket.receive(packet);
        ByteBuffer inputBuffer = ByteBuffer.wrap(bytes);
        return inputBuffer.getInt();
    }


    public static Integer tryReceiveInt(DatagramSocket recSocket)
    {
        try {
            return receiveInt(recSocket);
        }catch(IOException e) {
            return null;
        }
    }



    public static boolean trySendInt(DatagramSocket outSocket, int toSend)
    {
        try {
            sendInt(outSocket, toSend);
            return true;
        }catch(IOException e){
            return false;
        }
    }
}
