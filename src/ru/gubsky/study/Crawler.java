/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.gubsky.study;

import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
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
public class Crawler
{
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
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        }
        Properties properties = new Properties();
        properties.setProperty("useUnicode", "true");
        properties.setProperty("characterEncoding", "utf8");

        String urlConnection = "jdbc:mysql://" + host + ":" + port + "/"
                + db + "?user=" + login + "&password=" + passw;
        conn_ = DriverManager.getConnection(urlConnection, properties);
        stat_ = conn_.createStatement();
        String setnames = "set names \'utf8\';";
        stat_.execute(setnames);

        this.createIndexTables();
    }

    public void calculatePageRank() throws SQLException
    {
        stat_.execute("DROP TABLE IF EXISTS pagerank");
        String queryCreateTable = "CREATE TABLE pagerank("
                + "url_id INTEGER NOT NULL,"
                + "pr DOUBLE PRECISION"
                + ") CHARACTER SET utf8;";
        stat_.execute(queryCreateTable);

        String queryGetUrls = "select row_id from url_list";
        ResultSet rs = stat_.executeQuery(queryGetUrls);
        while (rs.next()) {
            int urlId = rs.getInt(1);
            String query = "insert into pagerank values(?, ?)";
            PreparedStatement ps = conn_.prepareStatement(query);
            ps.setInt(1, urlId);
            ps.setDouble(2, 1.0);
            ps.execute();
        }
        

        final int ITER_COUNT = 20;
        for (int i = 0; i < ITER_COUNT; i++) {
            System.out.println("i: " + i);
            rs.beforeFirst();
            while (rs.next()) {
                int urlId = rs.getInt(1);

                String query =
                        "select 0.15 + 0.85 * sum(r.divi) as pr from "
                        + "("
                        + "select p.pr, fr.to_id, fr.from_id, fr.count, p.pr / fr.count as divi "
                        + "from"
                        + "    pagerank p,"
                        + "    (select f.to_id, f.from_id, count(f.from_id) as count "
                        + "    from"
                        + "        link l2,"
                        + "        (select * from link where to_id = ?) as f"
                        + "    where l2.from_id = f.from_id group by l2.from_id) as fr "
                        + "where p.url_id = fr.to_id"
                        + ") as r;";
                PreparedStatement ps = conn_.prepareStatement(query);
                
                ps.setInt(1, urlId);
//                System.out.println(ps);
                ResultSet rsPr = ps.executeQuery();
                if (rsPr.next()) {
                    double pr = rsPr.getDouble(1);
//                    System.out.println(pr);
                    String qUpd = "update pagerank set pr = ? where "
                            + "url_id = ?;";
                    PreparedStatement psUpd = conn_.prepareStatement(qUpd);
                    psUpd.setDouble(1, pr);
                    psUpd.setInt(2, urlId);
//                    System.out.println(psUpd);
                    psUpd.executeUpdate();
                }
                
            }
        }

    }


    /*
     * Вспомогательная функция для получения идентификатора и добавления записи,
     * если такой еще нет подходит для url_list
     */
    private int getEntryId(String table, String field, String value,
            boolean createNew) throws SQLException
    {
        int result = NOTCREATED;

        String sqlSelect = "SELECT row_id FROM " + table + " WHERE "
                + field + " = ?;";
        PreparedStatement preps = conn_.prepareStatement(sqlSelect);
        preps.setString(1, value);
        ResultSet rs = preps.executeQuery();

        if (rs.next()) {
            result = rs.getInt("row_id");
            rs.close();
        } else if (createNew == false) {
            result = NOTCREATED;
            rs.close();
        } else {
            String sqlInsert = "INSERT INTO " + table + "(" + field + ") "
                    + " VALUES(?);";
            PreparedStatement ps = conn_.prepareStatement(sqlInsert, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, value);
            try {
                ps.executeUpdate();
                rs = ps.getGeneratedKeys();
                if (rs.next()) {
                    result = rs.getInt(1);
                }
            } catch (SQLException ex) {
                System.out.println("ex: " + ex.getMessage());
                System.out.println("word: " + value);
            } finally {
                rs.close();
            }
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
        String[] words = Utils.separateWords(text);
        //Получаем идентификатор URL
        int urlId = this.getEntryId(URLLIST_TABLE, "url", url, true);
        System.out.println("words: " + words.length);

        //Связать каждое слово с этим URL
        long timeStart = System.currentTimeMillis();
        for (int i = 0; i < words.length; i++) {
            addWordLocation(urlId, words[i], i);
        }
        long timeEnd = System.currentTimeMillis();
        System.out.println("add words location time: " + (timeEnd - timeStart));
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
     * Проиндексирован ли URL
     */
    private boolean isIndexed(String url) throws SQLException
    {
        int urlId = getEntryId(URLLIST_TABLE, "url", url, false);
        if (urlId == NOTCREATED) {
            return false;
        }
        String query = "select word from word_location WHERE url_id=" + urlId + "";
        ResultSet rs = stat_.executeQuery(query);
        if (rs.next()) {
            rs.close();
            return true;
        } else {
            rs.close();
            return false;
        }
    }

    private void addWordLocation(int urlId, String word, int location) throws SQLException
    {
        String query = "INSERT INTO word_location VALUES(?, ?, ?)";
        PreparedStatement ps = conn_.prepareStatement(query);
        ps.setInt(1, urlId);
        ps.setString(2, word);
        ps.setInt(3, location);
        ps.executeUpdate();
    }

    /*
     * Добавление ссылки с одной страницы на другую
     */
    private void addLinkRef(String urlFrom, String urlTo, String linkText, boolean createUrlTo) throws SQLException
    {
        int idUrlFrom = this.getEntryId(URLLIST_TABLE, "url", urlFrom, true);
        int idUrlTo = this.getEntryId(URLLIST_TABLE, "url", urlTo, createUrlTo);

//        if (idUrlFrom == NOTCREATED || idUrlTo == NOTCREATED) {
//            return;
//        }
        String query = "INSERT INTO link(from_id, to_id) VALUES(?, ?)";
        PreparedStatement ps = conn_.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        ps.setInt(1, idUrlFrom);
        ps.setInt(2, idUrlTo);
        ps.executeUpdate();

        ResultSet rs = ps.getGeneratedKeys();
        int linkId = -1;
        if (rs.next()) {
            linkId = rs.getInt(1);
        }

        // words
        String[] words = Utils.separateWords(linkText);
        for (int i = 0; i < words.length; i++) {
            String queryWord = "INSERT INTO link_words VALUES(?, ?);";
            PreparedStatement ps2 = conn_.prepareStatement(queryWord);
            ps2.setString(1, words[i]);
            ps2.setInt(2, linkId);
            ps2.executeUpdate();
        }
    }

    /*
     * Непосредственно сам метод сбора данных. Начиная с заданного списка
     * страниц, выполняет поиск в ширину до заданной глубины, индексируя все
     * встречающиеся по пути страницы
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
                long timeStart = System.currentTimeMillis();

                System.out.println("====\nURL: " + currentURL);
                this.addToIndex(currentURL, doc);
                //получить список ссылок со страницы
                Elements links = doc.select("a[href]");
                System.out.println("links: " + links.size());
                // берем все ссылки со страницы
                long linkTimeStart = System.currentTimeMillis();
                for (Element l : links) {
                    final int MIN_LINKURL_SIZE = 5;
                    String linkURL = l.attr("abs:href");
                    String linkText = l.text();

                    if (linkURL.length() < MIN_LINKURL_SIZE) {
                        continue;
                    }

                    // @todo: якорь? get параметры?

                    if (i + 1 < depth) {
                        if (isIndexed(linkURL) == false) {
                            newPages.add(linkURL);
                        }
                        addLinkRef(currentURL, linkURL, linkText, true);
                    } else {
                        addLinkRef(currentURL, linkURL, linkText, false);
                    }
                }
                System.out.println("addLinkRef time: " + (System.currentTimeMillis() - linkTimeStart));
                long timeEnd = System.currentTimeMillis();
                System.out.println("all time: " + (timeEnd - timeStart));

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
            + "row_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,"
            + "url TEXT CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL) CHARACTER SET utf8;",
            "CREATE TABLE IF NOT EXISTS word_location("
            + "url_id INTEGER NOT NULL,"
            + "word TEXT CHARACTER SET utf8 COLLATE utf8_general_ci,"
            + "location INTEGER NOT NULL) CHARACTER SET utf8;",
            "CREATE TABLE IF NOT EXISTS link("
            + "row_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,"
            + "from_id INTEGER NOT NULL,"
            + "to_id INTEGER NOT NULL) CHARACTER SET utf8;",
            "CREATE TABLE IF NOT EXISTS link_words("
            + "word TEXT CHARACTER SET utf8 COLLATE utf8_general_ci,"
            + "link_id INTEGER NOT NULL) CHARACTER SET utf8;"
        };
        for (String q : query) {
            stat_.executeUpdate(q);
        }
    }
}