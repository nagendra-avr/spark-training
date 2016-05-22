package com.training.spark.streaming;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Random;


/**
 * Created by Nagendra Amalakanta on 5/22/16.
 */
public class DataSocketServer {

    private static final String[] DATA = { "One, two, buckle my shoe",
            "Three, four, shut the door", "Five, six, pick up sticks",
            "Seven, eight, lay them straight", "Nine, ten, a big fat hen" };

    /**
     * Runs the server.
     */
    public static void main(String[] args) throws IOException {
        ServerSocket listener = new ServerSocket(9090);
        try {
            while (true) {
                Socket socket = listener.accept();
                try {
                    PrintWriter out =
                            new PrintWriter(socket.getOutputStream(), true);
                    Random random = new Random();
                    int index = random.nextInt(DATA.length);
                    out.println(DATA[index]);
                } finally {
                    socket.close();
                }
            }
        }
        finally {
            listener.close();
        }
    }
}
