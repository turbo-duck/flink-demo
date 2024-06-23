package icu.wzk.demo03;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class RandomNumClient extends Thread {

    @Override
    public void run() {
        String ip = "0.0.0.0";
        int port = 9999;
        try {
            ServerSocket serverSocket = new ServerSocket();
            InetSocketAddress address = new InetSocketAddress(ip, port);
            serverSocket.bind(address);
            Socket socket = serverSocket.accept();
            OutputStream output = socket.getOutputStream();
            PrintWriter writer = new PrintWriter(output, true);
            Random random = new Random();
            for (int i = 0; i < 500; i ++) {
                int randomNumber = random.nextInt(10) + 1;
                writer.println(randomNumber);
                System.out.println("ServerSocket Send To Flink: " + randomNumber);
                Thread.sleep(200);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
