/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.gubsky.study;

import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.Properties;
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
        final boolean needCrawl = true;
        final boolean needCalcRank = false;
        
        Properties sqlProperties = new Properties();
        sqlProperties.setProperty("server", "localhost");
        sqlProperties.setProperty("port", "3306");
        sqlProperties.setProperty("user", "root");
        sqlProperties.setProperty("pass", "12345");
        sqlProperties.setProperty("db", "lab2_db_full");
        
        Properties oldProperties = new Properties(sqlProperties);
        oldProperties.setProperty("db", "lab2_db");
        
        if (needCrawl || needCalcRank) {
            try {
//                Crawler crawler = new Crawler(null, 0, null, null, "db_mysql");
//                Crawler crawler = new Crawler("localhost", 3306, "study_user", "12345", "study_db");
//                Crawler crawler = new Crawler("localhost", 3306, "root", "12345", "lab2_db");
                Crawler crawler = new Crawler(sqlProperties);

                if (needCrawl) {
//                    String[] pages = new String[] {"http://yandex.ru", "http://lenta.ru", "http://novayagazeta.ru",
//                    "http://gazeta.ru", "http://ngs.ru", "http://nstu.ru", "http://ru.wikipedia.org/wiki/"};
                        String[] pages = new String[] {"http://yandex.ru"};
                    try {
                        crawler.crawl(pages, 3);
                    } catch (MalformedURLException ex) {
                        Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (IOException ex) {
                        Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                if (needCalcRank) {
                    crawler.calculatePageRank();
                }
            } catch (SQLException ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            // Search
//            String searchStr = "вышел блоги словари";
            String searchStr = "блоги яндекс";
            try {
                Searcher searcher = new Searcher(oldProperties);
                searcher.query(searchStr);
            } catch (SQLException ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
