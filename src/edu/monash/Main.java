package edu.monash;

public class Main {

    public static void main(String[] args) {
        // write your code here
        // Stream Rate is in number/seconds
        StreamingClient streamingClient = new StreamingClient();
        boolean success = streamingClient.ETLData();
        if (success) {
                streamingClient.strategicReadingAndStreamingFromMemory("1:1:1:1");
        }
    }
}
