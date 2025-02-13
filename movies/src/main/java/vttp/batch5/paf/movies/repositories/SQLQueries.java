package vttp.batch5.paf.movies.repositories;

public class SQLQueries {

    public static final String getMoviesCount_SQL = "select count(*) as count from imdb;";

    public static final String insertMovie_SQL = 
        """
            insert into imdb(imdb_id, vote_average, vote_count, release_date, revenue, budget, runtime)
            values(?,?,?,?,?,?,?)
            
        """;
}
