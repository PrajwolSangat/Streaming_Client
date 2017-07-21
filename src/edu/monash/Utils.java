package edu.monash;

import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Created by psangats on 21/07/2017.
 */
public class Utils {

    public static Object getValueFromArrayList(ArrayList list, int index) {
        if (list.size() > index) {
            return list.get(index);
        }
        return null;
    }

    public static Integer getSizeOfLargestList(ArrayList<String>... lists) {
        Integer largest = 0;

        for (ArrayList<String> list : lists
                ) {
            if (list.size() > largest) {
                largest = list.size();
            }
        }
        return largest;
    }
}
