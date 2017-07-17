package edu.monash;

public class Main {

    public static void main(String[] args) {
        // write your code here
        // Stream Rate is in number/seconds
        StreamingClient streamingClient = new StreamingClient();
        streamingClient.startStreaming(ReadingStrategy.SEQUENTIAL, Algorithm.EHJOIN, 25);
        //test1();
    }

    /*
     * Test to verify using threads creates random streams
     */
    public static void test1(){
        Thread t1 = new Thread(() -> threadTest(500));
        t1.start();
        Thread t2 = new Thread(() -> threadTest(200));
        t2.start();
        Thread t3 = new Thread(() -> threadTest(100));
        t3.start();
        Thread t4 = new Thread(() -> threadTest(229));
        t4.start();
    }

    public static void threadTest(Integer upperEnd){
        Integer sum = 0;
        for(int i = 1; i< upperEnd; i++){
            sum = sum + i;
            System.out.println(upperEnd + " " + sum);
        }
    }
}
