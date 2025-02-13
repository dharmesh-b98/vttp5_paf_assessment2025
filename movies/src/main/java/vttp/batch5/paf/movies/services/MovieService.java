package vttp.batch5.paf.movies.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.json.JsonArray;
import net.sf.jasperreports.json.data.JsonDataSource;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;

@Service
public class MovieService {

  @Autowired MongoMovieRepository mongoRepo;

  // TODO: Task 2
  

  // TODO: Task 3
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public JsonArray getProlificDirectors(Integer n) {
    return mongoRepo.getTopNDirectors(n);
  }


  // TODO: Task 4
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void generatePDFReport() {
    //JsonDataSource reportDS = new JsonDataSource();

  }

}
