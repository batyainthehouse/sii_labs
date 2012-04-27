/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.gubsky.study;


import com.sun.crypto.provider.RSACipher;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
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
    private int getEntryId(String table, String field, String value,
    boolean createNew) throws SQLException 
    {
        int result = 0;
        
        String sql = "SELECT row_id FROM " + table + " WHERE " + field + " = "
            + value + ";";
        ResultSet rs = stat_.executeQuery(sql);
     
        if (rs.next()) {
            result = rs.getInt("row_id");
        } else {
            sql = "INSERT INTO table 
        }
        
        return result;
        //1) проверить, есть ли значение value уже в таблице
        // 2) Если нет, то вставить и вернуть айдишник записи
        // 3) Если есть, то вернуть айдишник выбранной записи
    }

    
    
    /*
    * Индексирование одной страницы
    */   
    private void addToIndex(String url, Document doc) 
    {
        if (isIndexed(url)) {
            return;
        }
        
        //Получаем список слов из индексируемой страницы
        String text = this.getTextOnly(doc);
        String [] words = this.separateWords(text);
        //Получаем идентификатор URL
        int urlId = this.getEntryId("urllist", "url", url, true);
        //Связать каждое слово с этим URL
        for (int i=0; i < words.length; i++) {
            String word = words[i];
            //2) то добавляем запись в таблицу wordlist
            int worded = this.getEntryId(“wordlist”, “word”, word, true);
            //3) добавляем запись в wordlocation
        }

    }
   
    /*
    * Разбиение текста на слова
    */
    private String getTextOnly(Document doc) 
    {
        String bodyText = doc.body().text();
        StringBuffer resultBuffer = new StringBuffer(bodyText);
        
        // add alt atribute from images
        Elements imgs = doc.body().select("img[alt]");
        for (Element image : imgs) {
            resultBuffer.append(image.text());
        }
        System.out.println(resultBuffer.toString());
        return resultBuffer.toString();
    }
    
    /*
    * Разбиение текста на слова
    */
    private String[] separateWords(String text) 
    {
        StringTokenizer str = new StringTokenizer(text, " \t\n\r\f,!?;");
        String[] words = new String[str.countTokens()];
        for (int i = 0; i < words.length; i++) {
            words[i] = str.nextToken();
        }
        return words;
    }
    
    /*
    * Проиндексирован ли URL
    */
    private boolean isIndexed(String url1) 
    {
        return false;
    }
    
    /*
    * Добавление ссылки с одной страницы на другую
    */
    private void addLinkRef(String urlFrom, String urlTo, String linkText) 
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
            ArrayList<String> newPages = new ArrayList<String>();
            
            for (int k = 0; k < pages.length(); k++) {
                newPages.add(pages[i]);
            }
            
            for (int j = 0; j < newPages.size(); j++) {
                String currentURL = newPages.get(j);
                //получить содержимое страницы
                Document doc = Jsoup.connect(currentURL).get();
                //добавляем страницу в индекс
                this.addToIndex(currentURL, doc);
                //получить список ссылок со страницы
                Elements links = doc.select("a[href]");
                
                // берем все ссылки со страницы
                for (Element l : links) {
                    final int MIN_LINKURL_SIZE = 5;
                    String linkURL = links.attr("href");
                    String linkText = links.text();
                    
                    if (linkURL.length() < MIN_LINKURL_SIZE) {
                        continue;
                    }
                    
                    // todo: якорь? get параметры?
                   
                    if (isIndexed(url)) {
                        continue;
                    }
                    
                    newPages.add(linkURL);
                    addLinkRef(currentURL, linkURL, linkText);   
                }
            }
        }
    }
    
    /*
    * Инициализация таблиц в БД
    */
    private void createIndexTables()
    {
        final String[] query = new String[] {
                "CREATE TABLE IF NOT EXISTS url_list("
                +"row_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"
                +"from_id INTEGER NOT_NULL,"
                +"to_id INTEGER NOT_NULL,"
                + "description TEXT);",
                
                "CREATE TABLE IF NOT EXISTS word_list("
                + "row_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"
                + "word TEXT NOT NULL);",
                
                "CREATE TABLE IF NOT EXISTS word_location("
                + "url_id INTEGER NOT NULL,"
                + "word_id INTEGER NOT NULL,"
                + "location INTEGER NOT NULL);",
                
                "CREATE TABLE IF NOT EXISTS link("
                + "row_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"
                + "from_id INTEGER NOT NULL,"
                + "to_id INTEGER NOT NULL);",
                
                "CREATE TABLE IF NOT EXISTS link_words("
                + "word_id INTEGER NOT NULL,"
                + "link_id INTEGER NOT NULL);"
        };           
        try {
            for (String q : query) {
                stat_.executeUpdate(q);
            }
        } catch (SQLException ex) {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        }
              
    }


}