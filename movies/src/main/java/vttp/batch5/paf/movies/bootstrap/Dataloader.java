package vttp.batch5.paf.movies.bootstrap;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import jakarta.json.Json;

import jakarta.json.JsonObject;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;


@Component
public class Dataloader implements CommandLineRunner{

    @Autowired MongoMovieRepository mongoRepo;
    @Autowired MySQLMovieRepository sqlRepo;

    @Override
    public void run(String... args) throws Exception {
      loadMovies("test");
    }

    //TODO: Task 2
    public void loadMovies(String filepath) throws IOException, ParseException{
      if (checkSQLUpdated() == false && checkMongoUpdated() == false){
        List<Document> moviesDocList = getMoviesDocList(filepath);
        batchInsertMovies(moviesDocList);
      }  
    }

    public void batchInsertMovies(List<Document> moviesDocList) {
      Integer offset = 0;
      Integer batchsize = 25;
      
      while (offset < moviesDocList.size()){
        Integer endIndex = Math.min(offset + batchsize, moviesDocList.size());
        
        try{insertMovies(moviesDocList.subList(offset, endIndex));}
        catch(Exception e){
          System.out.println("ERROR" + offset+" " + endIndex);
          mongoRepo.logError(moviesDocList.subList(offset, endIndex), e.getMessage(), new Date());
          offset=endIndex;
        }
        offset=endIndex;
      }
    }

    @Transactional
    public void insertMovies(List<Document> moviesDocList){
      sqlRepo.batchInsertMovies(moviesDocList);
      mongoRepo.batchInsertMovies(moviesDocList);
    }

    public Boolean checkMongoUpdated(){
      return mongoRepo.checkDataLoaded();
    }


    public Boolean checkSQLUpdated(){
      return sqlRepo.checkDataLoaded();
    }



    public List<Document> getMoviesDocList(String filepath) throws IOException, ParseException{ // NEED TO CHANGE TO FILEPATH
      // "C:/Users/dharm/paf_b5_assessment_template/data/movies_post_2010.zip"
      // include zip input stream also
      ZipFile zipFile = new ZipFile(new File("C:/Users/dharm/paf_b5_assessment_template/data/movies_post_2010.zip"));
      ZipEntry zipEntry = zipFile.getEntry("movies_post_2010.json");
      
      InputStream stream = zipFile.getInputStream(zipEntry);
      InputStreamReader isr = new InputStreamReader(stream);
      BufferedReader br = new BufferedReader(isr);

      List<Document> documentList = new ArrayList<>();
      String line;
      while ((line = br.readLine()) != null){
        JsonObject movieJson = Json.createReader(new StringReader(line)).readObject();
        if (isDateAfter2018(movieJson)){
          continue;
        }
        Document document = Document.parse(movieJson.toString());
        document = makeValid(document);
        documentList.add(document);
      }
      //System.out.println(documentList.size() + " DOCLIST SIZE");
      return documentList;
    }



    private Boolean isDateAfter2018(JsonObject movieJson) throws ParseException{
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

      Date date = sdf.parse(movieJson.getString("release_date"));
      Date datelimit = sdf.parse("2017-12-31");

      if (date.getTime()>datelimit.getTime()){
        return true;
      }
      return false;
      
    }



    private Document makeValid(Document moviedoc){
      Document cleanedDoc = new Document();

      cleanedDoc.put("title",moviedoc.get("title",""));
      cleanedDoc.put("vote_average",moviedoc.get("vote_average",0));
      cleanedDoc.put("vote_count",moviedoc.get("vote_count",0));
      cleanedDoc.put("status",moviedoc.get("status", ""));
      cleanedDoc.put("release_date",moviedoc.get("release_date",""));
      cleanedDoc.put("revenue",moviedoc.get("revenue",0));
      cleanedDoc.put("runtime",moviedoc.get("runtime",0));
      cleanedDoc.put("budget",moviedoc.get("budget",0));
      cleanedDoc.put("imdb_id",moviedoc.get("imdb_id",""));
      cleanedDoc.put("original_language",moviedoc.get("original_language",""));
      cleanedDoc.put("overview",moviedoc.get("overview",""));
      cleanedDoc.put("popularity",moviedoc.get("popularity",0));
      cleanedDoc.put("tagline",moviedoc.get("tagline",""));
      cleanedDoc.put("genres",moviedoc.get("genres",""));
      cleanedDoc.put("spoken_languages",moviedoc.get("spoken_languages",""));
      cleanedDoc.put("casts",moviedoc.get("casts",""));
      cleanedDoc.put("director",moviedoc.get("director",""));
      cleanedDoc.put("imdb_rating",moviedoc.get("imdb_rating",0));
      cleanedDoc.put("imdb_votes",moviedoc.get("imdb_votes",0));
      cleanedDoc.put("poster_path",moviedoc.get("poster_path",""));
      
      return cleanedDoc;
    }

    



}
