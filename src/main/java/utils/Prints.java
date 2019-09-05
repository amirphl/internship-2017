package utils;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by Ali on 9/4/17.
 */
public class Prints {
    public static String getPrintStackTrace(Exception e) {
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }
}
