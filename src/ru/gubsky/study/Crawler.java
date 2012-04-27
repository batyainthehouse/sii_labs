/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.gubsky.study;


import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.*;
import java.util.ArrayList;
import java.util.StringTokenizer;
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
    
    private static final String URLLIST_TABLE = "url_list";
    private static final String WORDLIST_TABLE = "word_list";
    private static final int NOTCREATED = -1;
    
    /*
    * Инициализация паука с параметрами БД
    */
    /**
     * 
     * @param host
     * @param port
     * @param login
     * @param passw
     * @param db
     * @throws SQLException
     */
    public Crawler(String host, int port, String login, String passw, String db) throws SQLException
    {
        //todo : use host, port!
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        }
        conn_ = DriverManager.getConnection("jdbc:mysql://localhost:3306/study_db?" +
                                   "user=root&password=12345");     
        stat_ = conn_.createStatement();
        this.createIndexTables(); 
    }

    /*
    * Вспомогательная функция для получения идентификатора и
    * добавления записи, если такой еще нет
    * подходит для url_list, word_list
    */
    private int getEntryId(String table, String field, String value,
    boolean createNew) throws SQLException 
    {
        int result;
        
        String sqlSelect = "SELECT row_id FROM " + table + " WHERE " + field + " = \'"
            + value + "\';";
        ResultSet rs = stat_.executeQuery(sqlSelect);
     
        if (rs.next()) {
            result = rs.getInt("row_id");
            rs.close();
        } else if (createNew == false) {
            result = NOTCREATED;
            rs.close();
        } else {   
            String sqlInsert = "INSERT INTO " + table + "(\'" + field + "\') "
                    + " VALUES(\'" + value + "\');";
            stat_.executeUpdate(sqlInsert);
            
            rs = stat_.getGeneratedKeys();
            if (rs.next()) {
                result = rs.getInt(1);
            } else {
                result = NOTCREATED;
            }
            rs.close();
        }
       
        return result;
    }

    
    
    /*
    * Индексирование одной страницы
    */   
    private void addToIndex(String url, Document doc) throws SQLException 
    {
        if (isIndexed(url)) {
            return;
        }
        
        //Получаем список слов из индексируемой страницы
        String text = this.getTextOnly(doc);
        String [] words = this.separateWords(text);
        //Получаем идентификатор URL
        int urlId = this.getEntryId(URLLIST_TABLE, "url", url, true);
        System.out.println("====\nURL: " + url + "\nwords: " + words.length);
        //Связать каждое слово с этим URL
        for (int i = 0, k = 0; i < words.length; i++, k++) {
            String word = words[i];
            //2) добавляем запись в таблицу wordlist
            int wordID = this.getEntryId(WORDLIST_TABLE, "word", word, true);
            //3) добавляем запись в wordlocation
            addWordLocation(urlId, wordID, i);
            
            if (k == 50) {
                k = 0;
                System.out.print(100 * i/words.length + "-");
            }
        }
        System.out.println("\n====\nURL: " + url + "\nword[0]:" + words[0]);

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
//        System.out.println(resultBuffer.toString());
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
    private boolean isIndexed(String url) throws SQLException 
    {
        int urlId = getEntryId(URLLIST_TABLE, "url", url, false);
        if (urlId == NOTCREATED) {
            return false;
        }
        String query = "select word_id from word_location WHERE url_id=" + urlId + "";
        ResultSet rs = stat_.executeQuery(query);
        if (rs.next()) {
            rs.close();
            return true;
        } else {
            rs.close();
            return false;
        }  
    }
    
    private void addWordLocation(int urlId, int wordId, int location) throws SQLException
    {
        String query = "INSERT INTO word_location VALUES("
                + urlId + ", " + wordId + ", " + location + ")";
        stat_.executeUpdate(query);
    }
    
    
    /*
    * Добавление ссылки с одной страницы на другую
    */
    private void addLinkRef(String urlFrom, String urlTo, String linkText) throws SQLException 
    {
        int idUrlFrom = this.getEntryId(URLLIST_TABLE, "url", urlFrom, true);
        int idUrlTo = this.getEntryId(URLLIST_TABLE, "url", urlTo, true);
        
        String query = "INSERT INTO link(from_id, to_id) VALUES(" + idUrlFrom 
                + ", " + idUrlTo + ")";
        stat_.executeUpdate(query);
        int linkId = stat_.getGeneratedKeys().getInt(1);
        
        
        // words
        String[] words = separateWords(linkText);
        String[] queryWords = new String[words.length];
        
        for (int i = 0; i < words.length; i++) {
            int idWord = this.getEntryId(WORDLIST_TABLE, "word", words[i], true);
            queryWords[i] = "INSERT INTO link_words VALUES(" + idWord 
                    + ", " + linkId + ");";           
        }
        String queryW = Utils.arrayToString(queryWords, "\n");
        String resQuery = "BEGIN;\n" + queryW + "\nCOMMIT;";
        System.out.println(resQuery);
        conn_.setAutoCommit(false);
        stat_.executeUpdate(resQuery);
        conn_.commit();
    }
    
    /*
    * Непосредственно сам метод сбора данных.
    * Начиная с заданного списка страниц, выполняет поиск в ширину
    * до заданной глубины, индексируя все встречающиеся по пути страницы
    */
    /**
     * 
     * @param pages
     * @param depth
     * @throws MalformedURLException
     * @throws IOException
     * @throws SQLException
     */
    public void crawl(String[] pages, int depth) throws MalformedURLException, IOException, SQLException 
    {
        ArrayList<String> curPages = new ArrayList<String>();   
        for (int k = 0; k < pages.length; k++) {
            curPages.add(pages[k]);
        }
        
        for (int i = 0; i < depth; i++) {
            ArrayList<String> newPages = new ArrayList<String>();
            
            for (int j = 0; j < curPages.size(); j++) {
                String currentURL = curPages.get(j);
                //получить содержимое страницы
                Document doc;
                try {
                    doc = Jsoup.connect(currentURL).get();
                } catch (java.lang.IllegalArgumentException e) {
                    System.out.println("illegal argument exception");
                    continue;
                } catch (IOException e) {
                    System.out.println("io exception");
                    continue;
                }
                //добавляем страницу в индекс
                this.addToIndex(currentURL, doc);
                //получить список ссылок со страницы
                Elements links = doc.select("a[href]");
                System.out.println("links on " + currentURL + ": " + links.size());
                // берем все ссылки со страницы
                for (Element l : links) {
                    final int MIN_LINKURL_SIZE = 5;
                    String linkURL = l.attr("href");
                    String linkText = l.text();
                    
                    if (linkURL.length() < MIN_LINKURL_SIZE) {
                        continue;
                    }
                    
                    // todo: якорь? get параметры?
                   
                    if (isIndexed(linkURL)) {
                        continue;
                    }
                    //System.out.println("new: " + linkURL);
                    newPages.add(linkURL);
                    addLinkRef(currentURL, linkURL, linkText);   
                }
            }
            System.out.println("size of newPages: " + newPages.size());
            curPages = newPages;
        }
    }
          
    
    /*
    * Инициализация таблиц в БД
    */
    private void createIndexTables() throws SQLException
    {
        final String[] query = new String[] {
                "CREATE TABLE IF NOT EXISTS url_list("
                +"row_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                +"url TEXT NOT NULL);",
                
                "CREATE TABLE IF NOT EXISTS word_list("
                + "row_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                + "word TEXT NOT NULL);",
                
                "CREATE TABLE IF NOT EXISTS word_location("
                + "url_id INTEGER NOT NULL,"
                + "word_id INTEGER NOT NULL,"
                + "location INTEGER NOT NULL);",
                
                "CREATE TABLE IF NOT EXISTS link("
                + "row_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                + "from_id INTEGER NOT NULL,"
                + "to_id INTEGER NOT NULL);",
                
                "CREATE TABLE IF NOT EXISTS link_words("
                + "word_id INTEGER NOT NULL,"
                + "link_id INTEGER NOT NULL);"
        };           
        for (String q : query) {
            stat_.executeUpdate(q);
        }              
    }


}