/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.gubsky.study;

import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ryd6l1f
 */
public class Searcher
{
    private Connection conn_;
    private Statement stat_;

    public Searcher(String host, int port, String login, String passw,
            String db) throws SQLException
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
    }

    public void query(String q) throws SQLException
    {
        String[] words = Utils.separateWords(q);
        String[] urls = getMatchRows(words);
        HashMap sortedUrls = getSortedList(urls, words);
    }

    private String[] getMatchRows(String[] words) throws SQLException
    {
        String queryString = getQueryString(words.length);
        PreparedStatement ps = getPreparedStatement(queryString, words);
        System.out.println(ps);
        ResultSet rs = ps.executeQuery();

        int size = Utils.getSizeOfResultSet(rs);
        System.out.println(size);
        String[] resUrls = new String[size];
        for (int i = 0; rs.next(); i++) {
            resUrls[i] = rs.getString(1);
            System.out.println(resUrls[i]);
        }

        return resUrls;
    }

    private String getQueryString(int wordsCount)
    {
        if (wordsCount < 1) {
            return null;
        }
        String resultString = "SELECT distinct u.row_id FROM url_list u, ("
                + "SELECT w.url_id FROM word_location w";
        String whereString = " where w.word = ?";
        for (int i = 1; i < wordsCount; i++) {
            resultString += ", (SELECT * FROM word_location WHERE word = ?) AS t" + i;
            whereString += " AND w.url_id = t" + i + ".url_id";
        }
        resultString += whereString + ") AS e WHERE u.row_id = e.url_id;";
        return resultString;
    }

    private PreparedStatement getPreparedStatement(String queryString, String[] words) throws SQLException
    {
        PreparedStatement ps = conn_.prepareStatement(queryString);
        for (int i = 0; i < words.length; i++) {
            ps.setString(i + 1, words[i]);
        }
        return ps;
    }

    private HashMap getSortedList(String[] rows, String[] words)
    {
        //инициализируем массив весов weights[]
        double weight[] = new double[rows.length];
        for (int i = 0; i < weight.length; i++) {
            System.out.println(weight[i]);
        }

        //рассчитывает веса для страниц и возвращает хеш-массив
        //url->вес
        return null;
    }

    private String getUrlName(int id) throws SQLException
    {
        String sqlSelect = "SELECT url FROM url_list WHERE row_id = ?";
        PreparedStatement preps = conn_.prepareStatement(sqlSelect);
        preps.setInt(1, id);
        ResultSet rs = preps.executeQuery();

        if (rs.next()) {
            return rs.getString(1);
        } else {
            return null;
        }
    }

    // @todo
    private HashMap normalizeScores(HashMap scores, boolean smallIsBetter, double divider)
    {
        HashMap resultHash = new HashMap(scores.size());
        Set keys = scores.keySet();
        Iterator iterator = keys.iterator();

        if (smallIsBetter) {
            while (iterator.hasNext()) {
                Object key = iterator.next();
                double w = (double) scores.get(key);
                w = 1 - w / divider;
                resultHash.put(key, w);
            }
        } else {
            while (iterator.hasNext()) {
                Object key = iterator.next();
                double w = (double)scores.get(key);
                w = w / divider;
                resultHash.put(key, w);
            }
        }
        return resultHash;
    }

    public HashMap frequencyScore(String[] rows)
    {
        //пройти по всем ссылкам
        //для каждой ссылки посчитать количество попаданий слов в запросе
        //вернуть нормализованный результат
        return null;
    }
}
