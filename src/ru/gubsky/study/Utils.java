/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.gubsky.study;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ryd6l1f
 */
public class Utils
{
    public static String arrayToString(String[] a, String separator)
    {
        StringBuffer result = new StringBuffer();
        if (a.length > 0) {
            result.append(a[0]);
            for (int i = 1; i < a.length; i++) {
                result.append(separator);
                result.append(a[i]);
            }
        }
        return result.toString();
    }

    /*
     * Разбиение текста на слова
     */
    public static String[] separateWords(String text)
    {
        StringTokenizer str = new StringTokenizer(text, " \t\n\r\f,!?;");
        String[] words = new String[str.countTokens()];
        for (int i = 0; i < words.length; i++) {
            words[i] = str.nextToken();
        }
        return words;
    }

    public static int getSizeOfResultSet(ResultSet rs)
    {
        int size = 0;
        if (rs != null) {
            try {
                rs.last();
                size = rs.getRow();
                rs.beforeFirst();
            } catch (SQLException ex) {
                Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return size;
    }
}
