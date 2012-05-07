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
        final boolean needCrawl = false;
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
//                    "http://gazeta.ru", "http://ngs.ru", "http://nstu.ru", "http://ru.wikipedia.org/wiki/",
//                    "http://habrahabr.ru"};
//                    String[] pages = new String[] {"http://ru.wikipedia.org/wiki/дипломат", 
//                        "http://http://lurkmore.to/%D0%A1%D0%B5%D1%80%D1%8C%D1%91%D0%B7%D0%BD%D1%8B%D0%B9_%D0%B1%D0%B8%D0%B7%D0%BD%D0%B5%D1%81",
//                    "http://lurkmore.to/%D0%98%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%D1%8BЭ"};
//                    String[] pages = new String[] {"http://mail.ru", "http://otvety.google.ru/otvety/"};  
//                    String[] pages = new String[] {"http://yaca.yandex.ru/yca/geo/Russia/Siberian/Novosibirsk_Region/Novosibirsk/", 
//                        "http://yaca.yandex.ru/yca/cat/Employment/", "http://www.mk.ru/",
//                        "http://www.izvestia.ru/", "http://www.kommersant.ru/", "http://www.ng.ru/",
//                        "http://www.regnum.ru/"};
                    
                    String[] pages = new String[] {"http://www.cnews.ru", "http://utro.ru", "http://aif.ru", "http://www.newizv.ru"};
                    
                    try {
                        crawler.crawl(pages, 2);
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
                Searcher searcher = new Searcher(sqlProperties);
                searcher.query(searchStr);
            } catch (SQLException ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
