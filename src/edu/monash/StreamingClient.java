package edu.monash;

import java.io.DataOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.net.Socket;
import java.util.*;

/**
 * Created by psangats on 7/07/2017.
 */
public class StreamingClient {

    String dataDirectory = "TestData\\";
    //String dataDirectory = "C:\\Raw_Data_Source_For_Test\\StreamingAlgorithmTestData\\";
    //    String streamRFile = dataDirectory + "rStream.txt";
//    String streamSFile = dataDirectory + "sStream.txt";
//    String streamTFile = dataDirectory + "tStream.txt";
//    String streamUFile = dataDirectory + "uStream.txt";
    String streamRFile = dataDirectory + "RStreamCommon.txt";
    String streamSFile = dataDirectory + "SStreamCommon.txt";
    String streamTFile = dataDirectory + "TStreamCommon.txt";
    String streamUFile = dataDirectory + "UStreamCommon.txt";

    public void startStreaming(ReadingStrategy readingStrategy, Algorithm algorithm, Integer streamRate) {
        try {
            if (readingStrategy.equals(ReadingStrategy.RANDOM)) {
                /**
                 * Reading Strategy : Random
                 */
                Thread[] threadArray = new Thread[4];
                Thread threadR = new Thread(() -> readFromFile(streamRFile, "R", algorithm, streamRate, 10));
                //threadR.start();
                Thread threadS = new Thread(() -> readFromFile(streamSFile, "S", algorithm, streamRate, 20));
                //threadS.start();

                threadArray[0] = threadR;
                threadArray[1] = threadS;
                if (!algorithm.equals(Algorithm.EHJOIN)) {
                    Thread threadT = new Thread(() -> readFromFile(streamTFile, "T", algorithm, streamRate, 15));
                    //threadT.start();

                    Thread threadU = new Thread(() -> readFromFile(streamUFile, "U", algorithm, streamRate, 18));
                    //threadU.start();
                    threadArray[2] = threadT;
                    threadArray[3] = threadU;
                }

                // Ensures that all threads have completed the job.
                for (Thread thread: threadArray
                     ) {
                    thread.start();
                    thread.join();
                }

            } else {
                /**
                 * Reading Strategy : Sequential
                 */
                readFromFile(streamRFile, "R", algorithm, streamRate, 10);
                readFromFile(streamSFile, "S", algorithm, streamRate, 10);
                if (!algorithm.equals(Algorithm.EHJOIN)) {
                    readFromFile(streamTFile, "T", algorithm, streamRate, 10);
                    readFromFile(streamUFile, "U", algorithm, streamRate, 10);
                }
            }

            if (algorithm.equals(Algorithm.EHJOIN)) {
                sendMessageToServer("R:CLEANUP:CLEANUP:"+ Algorithm.EHJOIN);
            }
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }

    private void readFromFile(String filePath, String streamName, Algorithm algorithm, Integer streamRate, Integer timeToHold) {
        Scanner sc = null;
        Integer streamCounter = 0;
        try {
            Socket clientSocket = new Socket("localhost", 4000);
            OutputStream outputStream = clientSocket.getOutputStream();
            DataOutputStream outToServer = new DataOutputStream(outputStream);

            File file = new File(filePath);
            sc = new Scanner(file);
            while (sc.hasNextLine()) {
                //clientSocket.setKeepAlive(true);
                String[] values = sc.nextLine().split(" ");
                String dataToSend;

                // Only used in SLICE JOIN of Distinct Attribute Join
                if (streamName.equals("T") && algorithm.equals(Algorithm.SLICEJOIN)) {
                    dataToSend = streamName + ":" + values[1] + ":" + values[0] + ":" + algorithm;
                } else {
                    dataToSend = streamName + ":" + values[0] + ":" + values[1] + ":" + algorithm;
                }
                outToServer.writeBytes(dataToSend + '\n');
                outToServer.flush();
                /**
                 * Use code below if you need to read back from server
                 *
                 * BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 * String modifiedSentence = inFromServer.readLine();
                 * System.out.println(modifiedSentence);
                 */
//                Thread.sleep(timeToHold);
//                streamCounter++;
//                if (streamCounter >= streamRate) {
//                    System.out.println("[" + new Date() + "] Number of data sent: " + streamCounter);
//                    Thread.sleep(1000);
//                    streamCounter = 0;
//                    System.out.println("[" + new Date() + "] Resetting the count");
//                }
            }
            clientSocket.close();
        } catch (Exception ex) {
            System.out.println(ex.toString());
        } finally {
            sc.close();
        }
    }

    private void sendMessageToServer(String message){
        try{
            Socket clientSocket = new Socket("localhost", 4000);
            OutputStream outputStream = clientSocket.getOutputStream();
            DataOutputStream outToServer = new DataOutputStream(outputStream);
            outToServer.writeBytes(message + '\n');
            outToServer.flush();
            clientSocket.close();
        }
        catch (Exception ex)
        {
            System.out.println(ex.toString());
        }

    }
}
