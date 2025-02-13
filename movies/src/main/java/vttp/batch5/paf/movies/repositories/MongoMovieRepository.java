package vttp.batch5.paf.movies.repositories;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Date;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

@Repository
public class MongoMovieRepository{

    @Autowired MongoTemplate template;

    /*
    db.imdb.find().count()
    */
    public Boolean checkDataLoaded(){
        List<Document> doc = template.find(new Query(), Document.class, "imdb");
        Integer count = doc.size();
        if (count>0){
        return true;
        }
        return false;
    }
    



    // TODO: Task 2.3
    // You can add any number of parameters and return any type from the method
    // You can throw any checked exceptions from the method
    // Write the native Mongo query you implement in the method in the comments
    
    
    /*
    db.imdb.insertMany([
        {_id:<input imdb_id>},
        {title:<input title>},
        {directors:<input director>},
        {overview:<input overview>},
        {tagline:<input tagline>},
        {genres:<input genres>},
        {imdb_rating:<input imdb_rating>},
        {title:<input votes>}
    ])
    */
    public void batchInsertMovies(List<Document> docList) {
        List<Document> newDocList = new ArrayList<>();

        for (Document doc: docList){
            Document newDoc = new Document();
            newDoc.put("_id",doc.get("imdb_id"));
            newDoc.put("title", doc.get("title"));
            newDoc.put("directors", doc.get("director"));
            newDoc.put("overview",doc.get("overview"));
            newDoc.put("tagline",doc.get("tagline"));
            newDoc.put("genres", doc.get("genres"));
            newDoc.put("imdb_rating", doc.get("imdb_rating"));
            newDoc.put("imdb_votes", doc.get("imdb_votes"));

            newDocList.add(newDoc);
        }

        Collection<Document> newDocs = template.insert(newDocList,"imdb");
    }



    // TODO: Task 2.4
    // You can add any number of parameters and return any type from the method
    // You can throw any checked exceptions from the method
    // Write the native Mongo query you implement in the method in the comments
    //
    //    native MongoDB query here
    //
    public void logError(List<Document> docList, String error, Date timestamp) {
        List<String> ids = docList.stream().map(doc -> doc.getString("imdb_id")).toList();
        Document toInsert = new Document();
        toInsert.put("ids", ids.toString());
        toInsert.put("error", error);
        toInsert.put("timestamp",new Date());
        template.insert(toInsert, "errors");
    }



    // TODO: Task 3
    /*
    db.imdb.aggregate([
        {$group:{
            _id:"$director",
            movies_count:{$sum:1},
            total_revenue:{$sum:"$revenue"},
            total_budget:{$sum:"$budget"}
        }},
        {$sort:{
            movies_count:-1
        }},
        {$limit:n},
        {$project:{
            director_name:"$_id",
            movies_count:1,
            total_revenue:1,
            total_budget:1 
        }}
    ])
    */
    public JsonArray getTopNDirectors(Integer n){
        GroupOperation groupOperation = Aggregation.group("director")
                                                    .count().as("movies_count")
                                                    .sum("revenue").as("total_revenue")
                                                    .sum("budget").as("total_budget");

        SortOperation sortOperation = Aggregation.sort(Sort.by(Sort.Direction.DESC,"movies_count"));

        LimitOperation limitOperation = Aggregation.limit(n);

        ProjectionOperation projectionOperation = Aggregation.project("movies_count","total_revenue","total_budget")
                                                                .and("_id").as("director_name");

        Aggregation pipeline = Aggregation.newAggregation(groupOperation, sortOperation, limitOperation, projectionOperation);
        AggregationResults<Document> results= template.aggregate(pipeline, "imdb", Document.class);

        List<Document> resultDoc = results.getMappedResults();
        JsonArray resultJsonArray = docListtoJsonArray(resultDoc);

        return resultJsonArray;
    }


    public JsonArray docListtoJsonArray(List<Document> docList){
        JsonArrayBuilder jab = Json.createArrayBuilder();

        for (Document doc : docList){
            JsonObject docJson = Json.createReader(new StringReader(doc.toJson())).readObject();
            jab = jab.add(docJson);
        }
        JsonArray docJsonArray = jab.build();
        return docJsonArray;
    }


}
