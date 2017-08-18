package edu.monash;

public class Main {

    public static void main(String[] args) {
        // write your code here
        // Stream Rate is in number/seconds
        StreamingClient streamingClient = new StreamingClient();
        String strat_rate = null;
        if (args != null && args.length>0) {
            strat_rate = args[1];
            if (args.length > 2)
                streamingClient = new StreamingClient(args[0], args[2]);    // <directory> <letter>
            else
                streamingClient = new StreamingClient(args[0], "");    // <directory> <letter>
        } else {
            System.out.println("Insert parameters: <num tuples> <selectivity> <readStrategy|rate> [<r|s|t|u>]");
            System.exit(0);
        }
        boolean success = streamingClient.ETLData();
        if (success) {
//            streamingClient.strategicReadingAndStreamingFromMemory("1:1:1:1");
            if (strat_rate.contains(":"))
                streamingClient.strategicReadingAndStreamingFromMemory(strat_rate);
            else
                streamingClient.networkStreamingFromMemory(strat_rate);
        }
    }
}
