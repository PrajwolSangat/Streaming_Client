package edu.monash;

import java.io.DataOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Date;
import java.util.Scanner;

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
            if (readingStrategy.equals("RANDOM")) {
                /**
                 * Reading Strategy : Random
                 */

                Thread threadS = new Thread(() -> readFromFile(streamSFile, "S", algorithm, streamRate, 20));
                threadS.start();

                Thread threadR = new Thread(() -> readFromFile(streamRFile, "R", algorithm, streamRate, 10));
                threadR.start();

                if (!algorithm.equals("EHJ")) {
                    Thread threadT = new Thread(() -> readFromFile(streamTFile, "T", algorithm, streamRate, 15));
                    threadT.start();

                    Thread threadU = new Thread(() -> readFromFile(streamUFile, "U", algorithm, streamRate, 18));
                    threadU.start();
                }

            } else {
                /**
                 * Reading Strategy : Sequential
                 */
                readFromFile(streamRFile, "R", algorithm, streamRate, 10);
                readFromFile(streamSFile, "S", algorithm, streamRate, 10);
                readFromFile(streamTFile, "T", algorithm, streamRate, 10);
                readFromFile(streamUFile, "U", algorithm, streamRate, 10);
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
                String dataToSend = streamName + ":" + values[0] + ":" + values[1] + ":" + algorithm;
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
}
