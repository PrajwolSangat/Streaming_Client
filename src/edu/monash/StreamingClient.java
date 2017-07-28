package edu.monash;

import com.sun.org.apache.xpath.internal.SourceTree;

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
//    String streamRFile = dataDirectory + "RStreamCommon.txt";
//    String streamSFile = dataDirectory + "SStreamCommon.txt";
//    String streamTFile = dataDirectory + "TStreamCommon.txt";
//    String streamUFile = dataDirectory + "UStreamCommon.txt";
    String streamRFile = dataDirectory + "RStream.txt";
    String streamSFile = dataDirectory + "SStream.txt";
    String streamTFile = dataDirectory + "TStream.txt";
    String streamUFile = dataDirectory + "UStream.txt";

    ArrayList<String> streamR = new ArrayList<>();
    ArrayList<String> streamS = new ArrayList<>();
    ArrayList<String> streamT = new ArrayList<>();
    ArrayList<String> streamU = new ArrayList<>();
    private static String HOST_NAME = "localhost";
    private static Integer PORT = 4000;
    Integer timeToSleep = 50;
    private Algorithm algorithm;

    public void setAlgorithm(Algorithm algorithm)
    {
        this.algorithm = algorithm;
    }

    public void startStreaming(ReadingStrategy readingStrategy, Algorithm algorithm, Integer streamRate) {
        try {
            if (readingStrategy.equals(ReadingStrategy.RANDOM)) {
                /**
                 * Reading Strategy : Random
                 */
                Thread[] threadArray = new Thread[4];
                if (algorithm.equals(Algorithm.EHJOIN)) {
                    Thread threadR = new Thread(() -> readFromFile(streamSFile, "R", algorithm, streamRate, 10));
                    Thread threadS = new Thread(() -> readFromFile(streamRFile, "S", algorithm, streamRate, 20));
                    threadArray[0] = threadR;
                    threadArray[1] = threadS;
                } else {
                    Thread threadR = new Thread(() -> readFromFile(streamRFile, "R", algorithm, streamRate, 10));
                    Thread threadS = new Thread(() -> readFromFile(streamSFile, "S", algorithm, streamRate, 20));
                    Thread threadT = new Thread(() -> readFromFile(streamTFile, "T", algorithm, streamRate, 15));
                    Thread threadU = new Thread(() -> readFromFile(streamUFile, "U", algorithm, streamRate, 18));
                    threadArray[0] = threadR;
                    threadArray[1] = threadS;
                    threadArray[2] = threadT;
                    threadArray[3] = threadU;
                }
                // Ensures that all threads have completed the job.
                for (Thread thread : threadArray
                        ) {
                    thread.start();
                    thread.join();
                }
            } else {
                /**
                 * Reading Strategy : Sequential
                 */
                if (algorithm.equals(Algorithm.EHJOIN)) {
                    // To simulate  |R| <= |S|
                    if (readFromFile(streamSFile, "R", algorithm, streamRate, 1)) {
                        if (readFromFile(streamRFile, "S", algorithm, streamRate, 1)) {
                            sendMessageToServer("R:CLEANUP:CLEANUP:" + Algorithm.EHJOIN);
                        }
                    }
                } else {
                    readFromFile(streamRFile, "R", algorithm, streamRate, 10);
                    readFromFile(streamSFile, "S", algorithm, streamRate, 10);
                    readFromFile(streamTFile, "T", algorithm, streamRate, 10);
                    readFromFile(streamUFile, "U", algorithm, streamRate, 10);
                }
            }
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }

    public boolean ETLData() {
        boolean success = false;
        try {
            if (algorithm.equals(Algorithm.EHJOIN)) {
                readFromFileAndStoreInMemory(streamSFile, "R", algorithm);
                readFromFileAndStoreInMemory(streamRFile, "S", algorithm);
            } else {
                readFromFileAndStoreInMemory(streamRFile, "R", algorithm);
                readFromFileAndStoreInMemory(streamSFile, "S", algorithm);
                readFromFileAndStoreInMemory(streamTFile, "T", algorithm);
                readFromFileAndStoreInMemory(streamUFile, "U", algorithm);
            }
            success = true;
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
        return success;

    }

    public boolean strategicReadingAndStreamingFromMemory(String readingStrategy) {
        boolean streamingComplete = false;
        try {
            String[] _readingStrategy = readingStrategy.split(":");
            Integer denominatorR = Integer.parseInt(_readingStrategy[0]);
            Integer denominatorS = Integer.parseInt(_readingStrategy[1]);
            Integer denominatorT = Integer.parseInt(_readingStrategy[2]);
            Integer denominatorU = Integer.parseInt(_readingStrategy[3]);
            Integer countR = 0;
            Integer countS = 0;
            Integer countT = 0;
            Integer countU = 0;

            boolean rStreamComplete = false;
            boolean sStreamComplete = false;
            boolean tStreamComplete = false;
            boolean uStreamComplete = false;
            Socket clientSocket = new Socket(HOST_NAME, PORT);
            OutputStream outputStream = clientSocket.getOutputStream();
            DataOutputStream outToServer = new DataOutputStream(outputStream);

            while (!rStreamComplete || !sStreamComplete || !tStreamComplete || !uStreamComplete) {
                if (!rStreamComplete) {
                    for (int i = countR; i < streamR.size(); i++) {
                        if (i == 0 || i % denominatorR != 0) {
                            // System.out.println(Utils.getValueFromArrayList(streamR, i));
                            outToServer.writeBytes(streamR.get(i) + '\n');
                            outToServer.flush();
                            Thread.sleep(timeToSleep);
                            countR = i + 1;

                        } else {
                            // System.out.println(Utils.getValueFromArrayList(streamR, i));
                            outToServer.writeBytes(streamR.get(i) + '\n');
                            outToServer.flush();
                            Thread.sleep(timeToSleep);
                            break;
                        }

                    }
                    countR++;
                    if (streamR.size() == 0 || countR >= streamR.size()) {
                        rStreamComplete = true;
                        System.out.println("R Stream Completed.");
                    }
                }

                if (!sStreamComplete) {
                    for (int i = countS; i < streamS.size(); i++) {
                        if (i == 0 || i % denominatorS != 0) {
                            // System.out.println(Utils.getValueFromArrayList(streamS, i));
                            outToServer.writeBytes(streamS.get(i) + '\n');
                            outToServer.flush();
                            Thread.sleep(timeToSleep);
                            countS = i + 1;
                        } else {
                            // System.out.println(Utils.getValueFromArrayList(streamS, i));
                            outToServer.writeBytes(streamS.get(i) + '\n');
                            outToServer.flush();
                            Thread.sleep(timeToSleep);
                            break;
                        }
                    }
                    countS++;
                    if (streamS.size() == 0 || countS >= streamS.size()) {
                        sStreamComplete = true;
                        System.out.println("S Stream Completed.");
                    }
                }
                if (!tStreamComplete) {
                    for (int i = countT; i < streamT.size(); i++) {
                        if (i == 0 || i % denominatorT != 0) {
                            // System.out.println(Utils.getValueFromArrayList(streamT, i));
                            outToServer.writeBytes(streamT.get(i) + '\n');
                            outToServer.flush();
                            Thread.sleep(timeToSleep);
                            countT = i + 1;
                        } else {
                            // System.out.println(Utils.getValueFromArrayList(streamT, i));
                            outToServer.writeBytes(streamT.get(i) + '\n');
                            outToServer.flush();
                            Thread.sleep(timeToSleep);
                            break;
                        }
                    }
                    countT++;
                    if (streamT.size() == 0 || countT >= streamT.size()) {
                        tStreamComplete = true;
                        System.out.println("T Stream Completed.");
                    }

                }
                if (!uStreamComplete) {
                    for (int i = countU; i < streamU.size(); i++) {
                        if (i == 0 || i % denominatorU != 0) {
                            // System.out.println(Utils.getValueFromArrayList(streamU, i));
                            outToServer.writeBytes(streamU.get(i) + '\n');
                            outToServer.flush();
                            Thread.sleep(timeToSleep);
                            countU = i + 1;
                        } else {
                            // System.out.println(Utils.getValueFromArrayList(streamU, i));
                            outToServer.writeBytes(streamU.get(i) + '\n');
                            outToServer.flush();
                            Thread.sleep(timeToSleep);
                            break;
                        }
                    }
                    countU++;
                    if (streamU.size() == 0 || countU >= streamU.size()) {
                        uStreamComplete = true;
                        System.out.println("U Stream Completed.");
                    }
                }
            }
            streamingComplete = true;
            System.out.println("Streaming Complete.");
            if (algorithm.equals(Algorithm.EHJOIN)){
                outToServer.writeBytes("R:CLEANUP:CLEANUP:" + algorithm + '\n');
                outToServer.flush();
            }
            clientSocket.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return streamingComplete;
    }

    private boolean readFromFileAndStoreInMemory(String filePath, String streamName, Algorithm algorithm) {
        Scanner sc = null;
        boolean success = false;
        try {
            File file = new File(filePath);
            sc = new Scanner(file);
            while (sc.hasNextLine()) {
                String[] values = sc.nextLine().split(" ");
                String dataToSend = streamName + ":" + values[0] + ":" + values[1] + ":" + algorithm;
                switch (streamName) {
                    case "R":
                        streamR.add(dataToSend);
                        break;
                    case "S":
                        streamS.add(dataToSend);
                        break;
                    case "T":
                        streamT.add(dataToSend);
                        break;
                    case "U":
                        streamU.add(dataToSend);
                        break;
                }
            }
            success = true;
        } catch (Exception ex) {
            System.out.println(ex.toString());
        } finally {
            sc.close();

        }
        return success;
    }

    private boolean readFromFile(String filePath, String streamName, Algorithm algorithm, Integer streamRate, Integer timeToHold) {
        Scanner sc = null;
        Integer streamCounter = 0;
        boolean success = false;
        try {
            Socket clientSocket = new Socket(HOST_NAME, PORT);
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
                Thread.sleep(timeToHold);
//                streamCounter++;
//                if (streamCounter >= streamRate) {
//                    System.out.println("[" + new Date() + "] Number of data sent: " + streamCounter);
//                    Thread.sleep(1000);
//                    streamCounter = 0;
//                    System.out.println("[" + new Date() + "] Resetting the count");
//                }
            }
            clientSocket.close();
            success = true;
        } catch (Exception ex) {
            System.out.println(ex.toString());
        } finally {
            sc.close();

        }
        return success;
    }

    public void sendMessageToServer(String message) {
        try {
            Socket clientSocket = new Socket(HOST_NAME, PORT);
            OutputStream outputStream = clientSocket.getOutputStream();
            DataOutputStream outToServer = new DataOutputStream(outputStream);
            outToServer.writeBytes(message + '\n');
            outToServer.flush();
            //clientSocket.close();
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }

    }
}
