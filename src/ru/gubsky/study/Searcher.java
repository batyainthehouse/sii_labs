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

    public String[] getMatchRows(String q) throws SQLException
    {
        //Разбиваем поисковый запрос на слова по пробелам
        String[] words = Utils.separateWords(q);
        String queryString = getQueryString(words.length);
        PreparedStatement ps = getPreparedStatement(queryString, words);
        System.out.println(ps);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        
        return null;
    }

    private String getQueryString(int wordsCount)
    {
        if (wordsCount < 1) {
            return null;
        }
        String resultString = "SELECT u.url FROM url_list u, ("
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

    private HashMap getSortedList(String[] rows, int[] wordIds)
    {
        //инициализируем массив весов weights[]
        //рассчитывает веса для страниц и возвращает хеш-массив
        //url->вес
        return null;
    }

    private String getUrlName(int id)
    {
        //возвращает url по id из таблицы urllist
        return null;
    }

    private void query(String q)
    {
        //получает список документов по запросу q
        //с помощью метода getSortedList ранжирует их
        //возвращает ссылок отранжированных url
    }

    private HashMap normalizeScores(HashMap scores, boolean smallIsBetter)
    {
        double vSmall = 0.00001; //чтобы не делить на 0
        if (smallIsBetter) {
            Set keys = scores.keySet();
            Iterator iterator = keys.iterator();
            while (iterator.hasNext()) {
                double w = (double) scores.get(iterator.next());
//                w = 1 - w /
            }
        }
        //если меньший ранг лучше
        //для каждой ссылки 1 – scoreURL/minScore
        //если больший ранг лучше
        //для каждой ссылке
//        scoreURL / maxScore
        return null;
    }

    public HashMap frequencyScore(String[] rows)
    {
        //пройти по всем ссылкам
        //для каждой ссылки посчитать количество попаданий слов в запросе
        //вернуть нормализованный результат
        return null;
    }
}
