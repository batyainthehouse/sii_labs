/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.gubsky.study;

import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ryd6l1f
 */
public class Main
{
    public static void main(String[] arg)
    {
        try {
//            Crawler crawler = new Crawler(null, 0, null, null, "db_mysql");
            Crawler crawler = new Crawler(null, 0, null, null, null);
            String []pages = new String[]{"http://nstu.ru", "http://www.yandex.ru/"};
            try {
                crawler.crawl(pages, 2);
            } catch (MalformedURLException ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }   
    }
}
