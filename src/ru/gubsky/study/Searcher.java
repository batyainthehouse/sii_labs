/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.gubsky.study;

/**
 *
 * @author ryd6l1f
 */
public class Searcher
{
  public Searcher(String host, int port, String login, String passw,
          String db)
  {
  }

  public String[] getMatchRows(String q)
  {
    //Разбиваем поисковый запрос на слова по пробелам
    //Для каждого слова получаем его идентификатор в wordlist
    //Выбираем те ссылки links[], для которых есть связи со всеми
    // словами в запросе
    return links;
  }
}
