/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.gubsky.study;


import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

/**
 *
 * @author ryd6l1f
 */
public class Crawler {
    private Connection conn_;
    private Statement stat_;
    
    /*
    * Инициализация паука с параметрами БД
    */
    public Crawler(String host, int port, String login, String passw, String db) throws SQLException
    {
        //todo : use host, port!
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        }
        conn_ = DriverManager.getConnection("jdbc:sqlite:" + db, login, passw);
        stat_ = conn_.createStatement();
        this.createIndexTables(); 
    }

    /*
    * Вспомогательная функция для получения идентификатора и
    * добавления записи, если такой еще нет
    */
    public int getEntryId(String table, String field, String value, boolean
    createNew) 
    {
        return 1;
    }
    
    /*
    * Индексирование одной страницы
    */
    public boolean addToIndex(URL url, String html) 
    {
        return true;
    }
    
    private boolean addToIndex(URL page, Document doc) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
   
    /*
    * Разбиение текста на слова
    */
    public String getTextOnly(String text) 
    {
        return "";
    }
    
    /*
    * Разбиение текста на слова
    */
    public void separateWords(String text) 
    {
    
    }
    
    /*
    * Проиндексирован ли URL
    */
    public boolean isIndexed(URL url) 
    {
        return false;
    }
    
    /*
    * Добавление ссылки с одной страницы на другую
    */
    public void addLinkRef(URL urlFrom, URL urlTo, String linkText) 
    {
    
    }
    
    /*
    * Непосредственно сам метод сбора данных.
    * Начиная с заданного списка страниц, выполняет поиск в ширину
    * до заданной глубины, индексируя все встречающиеся по пути страницы
    */
    public void crawl(String[] pages, int depth) throws MalformedURLException, IOException 
    {
        for (int i = 0; i < depth; i++) {
            String[] newPagesSet = new String[50];
            for (int j = 0; j < pages.length; j++) {
                URL page = new URL(pages[j]);
                //получить содержимое страницы
                Document doc = Jsoup.connect(pages[j]).get();
                //добавляем страницу в индекс
                this.addToIndex(page, doc);
                //получить список ссылок со страницы
                Elements links = doc.select("a[href]");
                //1) обработать ссылки: убрать пустые,
                //вырезать якоря из ссылок
                //2) если ссылка еще не проиндексирована, то
                //добавить ссылки к newPageSet
                //3) получить из ссылок текст linkText
                //4) добавить в базу ссылку с одной страницы на другую
                //this.addLinkRef(page, link, linkText);
            }
            //расширить список обходимых документов
            pages = newPagesSet;
        }
    }
    /*
    * Инициализация таблиц в БД
    */
    private void createIndexTables()
    {
        final String[] query = new String[] {"CREATE TABLE IF NOT EXISTS url_list("
                +"row_id INTEGER NOT NULL,"
                +"from_id INTEGER NOT_NULL,"
                +"to_id INTEGER NOT_NULL);",
                
                "CREATE TABLE IF NOT EXISTS word_list("
                + "row_id INTEGER NOT NULL,"
                + "word TEXT NOT NULL);",
                
                "CREATE TABLE IF NOT EXISTS word_location("
                + "url_id INTEGER NOT NULL,"
                + "word_id INTEGER NOT NULL,"
                + "location INTEGER NOT NULL);",
                
                "CREATE TABLE IF NOT EXISTS link("
                + "row_id INTEGER NOT NULL,"
                + "from_id INTEGER NOT NULL,"
                + "to_id INTEGER NOT NULL);",
                
                "CREATE TABLE IF NOT EXISTS link_words("
                + "word_id INTEGER NOT NULL,"
                + "link_id INTEGER NOT NULL);"};           
        try {
            for (String q : query) {
                stat_.executeUpdate(q);
            }
        } catch (SQLException ex) {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        }
              
    }


}