package edu.monash;

public class Main {

    public static void main(String[] args) {
        // write your code here
        // Stream Rate is in number/seconds
        StreamingClient streamingClient = new StreamingClient();
        streamingClient.startStreaming(ReadingStrategy.RANDOM, Algorithm.SLICEJOIN, 25);
    }
}
