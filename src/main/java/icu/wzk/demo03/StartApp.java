package icu.wzk.demo03;


public class StartApp {

    public static void main(String[] args) throws Exception {
        RandomNumClient randomNumClient = new RandomNumClient();
        FlinkServer flinkServer = new FlinkServer();
        flinkServer.start();
        randomNumClient.start();
    }

}
