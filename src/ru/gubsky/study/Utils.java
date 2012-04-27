/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.gubsky.study;

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
        for (int i=1; i<a.length; i++) {
            result.append(separator);
            result.append(a[i]);
        }
    }
    return result.toString();
}
}
