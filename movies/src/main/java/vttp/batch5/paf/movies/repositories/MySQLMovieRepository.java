package vttp.batch5.paf.movies.repositories;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class MySQLMovieRepository {

  @Autowired JdbcTemplate template;


  public Boolean checkDataLoaded(){
    SqlRowSet rs = template.queryForRowSet(SQLQueries.getMoviesCount_SQL);
    rs.next();
    Integer count = rs.getInt("count");
    if (count>0){
      return true;
    }
    return false;
  }

  // TODO: Task 2.3
  // You can add any number of parameters and return any type from the method
  

  public void batchInsertMovies(List<Document>moviesDocList){
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    List<Object[]> params = moviesDocList.stream()
                                          .map(mdoc -> new Object[]{mdoc.getString("imdb_id"), mdoc.getInteger("vote_average"),
                                                                    mdoc.getInteger("vote_count"), mdoc.getString("release_date"),
                                                                    mdoc.getInteger("revenue"), mdoc.getInteger("budget"),
                                                                    mdoc.getInteger("runtime")})
                                          .collect(Collectors.toList());
    int added[] = template.batchUpdate(SQLQueries.insertMovie_SQL,params);
  }
  

 
  // TODO: Task 3


}
