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
    String streamRFile = dataDirectory + "rStream.txt";
    String streamSFile = dataDirectory + "sStream.txt";
    String streamTFile = dataDirectory + "tStream.txt";
    String streamUFile = dataDirectory + "uStream.txt";
    String streamVFile = dataDirectory + "vStream.txt";
    String streamFile = "";
    String streamLetter = "";
    private static String HOST_NAME = "localhost";
    private static Integer PORT = 4000;

    public StreamingClient() {
    }

    public StreamingClient(String dataDirectory, String streamLetter) {
        this.dataDirectory = dataDirectory;
        this.streamLetter = streamLetter;
        System.out.println("Data Directory: " + dataDirectory);
        System.out.println("Sending data to: " + HOST_NAME + ":" + PORT);
        streamFile = dataDirectory + streamLetter + "Stream.txt";
        streamRFile = dataDirectory + "rStream.txt";
        streamSFile = dataDirectory + "sStream.txt";
        streamTFile = dataDirectory + "tStream.txt";
        streamUFile = dataDirectory + "uStream.txt";
        streamVFile = dataDirectory + "vStream.txt";
    }

    ArrayList<String> streamAL = new ArrayList<>();
    ArrayList<String> streamR = new ArrayList<>();
    ArrayList<String> streamS = new ArrayList<>();
    ArrayList<String> streamT = new ArrayList<>();
    ArrayList<String> streamU = new ArrayList<>();
    ArrayList<String> streamV = new ArrayList<>();
//    private static String HOST_NAME = "localhost";

    Integer timeToSleep = 0;

    public boolean ETLData() {
        boolean success = false;
        try {
            if (streamLetter.equals("")) {
                readFromFileAndStoreInMemory(streamRFile, "R");
                readFromFileAndStoreInMemory(streamSFile, "S");
                readFromFileAndStoreInMemory(streamTFile, "T");
                readFromFileAndStoreInMemory(streamUFile, "U");
                readFromFileAndStoreInMemory(streamVFile, "V");
            } else {
                readFromFileAndStoreInMemory(streamFile, streamLetter.toUpperCase());
            }
            success = true;
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
        return success;
    }

    public boolean networkStreamingFromMemory(String rate) {
        boolean streamingComplete = false;
        try {
            System.out.println("Rate:" + rate);
            Socket clientSocket = new Socket(HOST_NAME, PORT);
            OutputStream outputStream = clientSocket.getOutputStream();
            DataOutputStream outToServer = new DataOutputStream(outputStream);
            long time0 = System.currentTimeMillis();
            for (int i = 0; i < streamAL.size(); i++) {
                outToServer.writeBytes(streamAL.get(i) + '\n');
                if (rate != null && i % Integer.valueOf(rate) == 0) {
                    outToServer.flush();
                    long elapsedTime = System.currentTimeMillis() - time0;
                    System.out.println(streamLetter.toUpperCase() + " Elapsed " + elapsedTime + " TIME:" + System.currentTimeMillis());
                    if (elapsedTime < 1000)
                        Thread.sleep(1000 - elapsedTime);
                    time0 = System.currentTimeMillis();
                }
            }
            System.out.println(streamLetter.toUpperCase() + " Stream Completed.");
            streamingComplete = true;
            if (streamLetter.toUpperCase().equals("R"))
            outToServer.writeBytes(streamLetter.toUpperCase() + ":COMPLETE:COMPLETE" + '\n');
            outToServer.flush();
            clientSocket.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return streamingComplete;
    }

    public boolean strategicReadingAndStreamingFromMemory(String readingStrategy) {
        boolean streamingComplete = false;
        try {
            String[] _readingStrategy = readingStrategy.split(":");
            Integer denominatorR = Integer.parseInt(_readingStrategy[0]);
            Integer denominatorS = Integer.parseInt(_readingStrategy[1]);
            Integer denominatorT = Integer.parseInt(_readingStrategy[2]);
            Integer denominatorU = Integer.parseInt(_readingStrategy[3]);
            Integer denominatorV = Integer.parseInt(_readingStrategy[4]);
            Integer countR = 0;
            Integer countS = 0;
            Integer countT = 0;
            Integer countU = 0;
            Integer countV = 0;

            boolean rStreamComplete = false;
            boolean sStreamComplete = false;
            boolean tStreamComplete = false;
            boolean uStreamComplete = false;
            boolean vStreamComplete = false;
            Socket clientSocket = new Socket(HOST_NAME, PORT);
            OutputStream outputStream = clientSocket.getOutputStream();
            DataOutputStream outToServer = new DataOutputStream(outputStream);

            while (!rStreamComplete || !sStreamComplete || !tStreamComplete || !uStreamComplete || !vStreamComplete) {
                if (!rStreamComplete) {
                    for (int i = countR; i < streamR.size(); i++) {
                        if (i == 0 || i % denominatorR != 0) {
                            // System.out.println(Utils.getValueFromArrayList(streamR, i));
                            outToServer.writeBytes(streamR.get(i) + '\n');
                            outToServer.flush();
//                            Thread.sleep(timeToSleep);
                            countR = i + 1;

                        } else {
                            // System.out.println(Utils.getValueFromArrayList(streamR, i));
                            outToServer.writeBytes(streamR.get(i) + '\n');
                            outToServer.flush();
//                            Thread.sleep(timeToSleep);
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
//                            Thread.sleep(timeToSleep);
                            countS = i + 1;
                        } else {
                            // System.out.println(Utils.getValueFromArrayList(streamS, i));
                            outToServer.writeBytes(streamS.get(i) + '\n');
                            outToServer.flush();
//                            Thread.sleep(timeToSleep);
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
//                            Thread.sleep(timeToSleep);
                            countT = i + 1;
                        } else {
                            // System.out.println(Utils.getValueFromArrayList(streamT, i));
                            outToServer.writeBytes(streamT.get(i) + '\n');
                            outToServer.flush();
//                            Thread.sleep(timeToSleep);
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
//                            Thread.sleep(timeToSleep);
                            countU = i + 1;
                        } else {
                            // System.out.println(Utils.getValueFromArrayList(streamU, i));
                            outToServer.writeBytes(streamU.get(i) + '\n');
                            outToServer.flush();
//                            Thread.sleep(timeToSleep);
                            break;
                        }
                    }
                    countU++;
                    if (streamU.size() == 0 || countU >= streamU.size()) {
                        uStreamComplete = true;
                        System.out.println("U Stream Completed.");
                    }
                }
                if (!vStreamComplete) {
                    for (int i = countV; i < streamV.size(); i++) {
                        if (i == 0 || i % denominatorV != 0) {
                            // System.out.println(Utils.getValueFromArrayList(streamU, i));
                            outToServer.writeBytes(streamV.get(i) + '\n');
                            outToServer.flush();
//                            Thread.sleep(timeToSleep);
                            countV = i + 1;
                        } else {
                            // System.out.println(Utils.getValueFromArrayList(streamU, i));
                            outToServer.writeBytes(streamV.get(i) + '\n');
                            outToServer.flush();
//                            Thread.sleep(timeToSleep);
                            break;
                        }
                    }
                    countV++;
                    if (streamV.size() == 0 || countV >= streamV.size()) {
                        vStreamComplete = true;
                        System.out.println("V Stream Completed.");
                    }
                }
            }
            streamingComplete = true;
            System.out.println("Streaming Complete.");
            outToServer.writeBytes("R:COMPLETE:COMPLETE" + '\n');
            outToServer.flush();
            clientSocket.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return streamingComplete;
    }

    private boolean readFromFileAndStoreInMemory(String filePath, String streamName) {
        Scanner sc = null;
        boolean success = false;
        try {
            File file = new File(filePath);
            sc = new Scanner(file);
            while (sc.hasNextLine()) {
                String[] values = sc.nextLine().split(" ");
                String dataToSend = streamName + ":" + values[0] + ":" + values[1];
                if (streamLetter.equals("")) {
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
                        case "V":
                            streamV.add(dataToSend);
                            break;
                    }
                } else {
                    streamAL.add(dataToSend);
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

    private boolean readFromFile(String filePath, String streamName, Integer streamRate, Integer timeToHold) {
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
                String dataToSend = streamName + ":" + values[0] + ":" + values[1];
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
}
