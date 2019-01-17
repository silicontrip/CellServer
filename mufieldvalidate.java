import java.io.BufferedReader;
import java.io.FileReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;

import org.json.*;
import com.google.common.geometry.*;

import org.bson.*;
import com.mongodb.*;
import com.mongodb.client.*;

public class mufieldvalidate
{

    private static S2LatLng locationToS2 (Document loc) throws NumberFormatException
    {
        if (loc.containsKey("latE6") && loc.containsKey("lngE6")) {
            return S2LatLng.fromDegrees( loc.getInteger("latE6") / 1000000.0, loc.getInteger("lngE6")/1000000.0);
        }
        throw new NumberFormatException("Object doesn't contain lat/lng");
    }

	private static String doctodt(Document f)
	{
		Document data = (Document) f.get("data");
		ArrayList<Document> capturedRegion = (ArrayList<Document>) data.get("points");
		return doctodt(capturedRegion);

	}
	private static String doctodt(ArrayList<Document> points)
        {
                Document vertexA = (Document) points.get(0);
                Document vertexB = (Document) points.get(1);
                Document vertexC = (Document) points.get(2);

                Integer latE6A = (Integer)vertexA.get("latE6");
                Integer lngE6A = (Integer)vertexA.get("lngE6");
                Integer latE6B = (Integer)vertexB.get("latE6");
                Integer lngE6B = (Integer)vertexB.get("lngE6");
                Integer latE6C = (Integer)vertexC.get("latE6");
                Integer lngE6C = (Integer)vertexC.get("lngE6");

                return new String ("{\"type\":\"polygon\",\"color\":\"#a24ac3\",\"latLngs\":[" +
                                        "{\"lat\":" + latE6A/1000000.0 + ",\"lng\":"+lngE6A/1000000.0+"},"+
                                        "{\"lat\":" + latE6B/1000000.0 + ",\"lng\":"+lngE6B/1000000.0+"},"+
                                        "{\"lat\":" + latE6C/1000000.0 + ",\"lng\":"+lngE6C/1000000.0+"}"+
                                        "]}");

        }

	private static UniformDistribution muForField (Document f, CellServer cs)
	{
		Document data = (Document) f.get("data");
		ArrayList<Document> capturedRegion = (ArrayList<Document>) data.get("points");

		S2Polygon thisField = cs.getS2Field(capturedRegion);

		return cs.muForField(thisField);
	}

    public static void main(String[] args)
    {
        try
        {

		MongoClient mongo;
		MongoDatabase db;
		MongoCursor<Document> cursor;
		MongoCollection<org.bson.Document> table;

		mongo = new MongoClient("localhost", 27017);
		db = mongo.getDatabase("ingressmu");
		table = db.getCollection("ingressmu");

		CellServer cs = new CellServer();
                
		JSONObject response = new JSONObject();

		cursor = table.find().iterator();

		int count = 0;
		int timetime = (int)(System.nanoTime() / 1000000000.0);
		int starttime = (int)(System.nanoTime() /1000000000.0);
// loop all fields
		while (cursor.hasNext()) {
                        Document entitycontent = cursor.next();
                        Integer score;
			// there is a support script to convert everything to a String
			// and then find out what's still writing integers

			score =  new Integer((String)entitycontent.get("mu"));

			// get me the field mu

			UniformDistribution fieldmu = muForField(entitycontent,cs);

			// validate the score
			if (fieldmu.getUpper() > 0)
				if (!fieldmu.roundAboveZero().contains(score))
				{
					System.out.println( "score: " + score + " -> " + fieldmu + " : [" + doctodt(entitycontent)  + "]");
					//System.out.println("");	
					//System.out.println("" + entitycontent);
					//System.out.println("");	
					ArrayList<Document>  splitFields = cs.findSplitField(entitycontent);
					//System.out.println("found: " + splitFields.size());
					//System.out.println("");	
					//System.out.println("");	
					if (splitFields.size() == 2)
					{
						// see if reversing the mu helps
						Integer muRec1 = new Integer((String) splitFields.get(0).get("mu"));
						UniformDistribution muEst1 = muForField(splitFields.get(0),cs);
						Integer muRec2 = new Integer((String) splitFields.get(1).get("mu"));
						UniformDistribution muEst2 = muForField(splitFields.get(1),cs);
					System.out.println( ": " + muRec1 + " -> " + muEst1 + " : [" + doctodt(entitycontent)  + "]");
					System.out.println( ": " + muRec2 + " -> " + muEst2 + " : [" + doctodt(entitycontent)  + "]");

						if (muEst1.roundAboveZero().contains(muRec2) && muEst2.roundAboveZero().contains(muRec1))
						{
							System.out.println ("SWAP");
							System.out.println("" + splitFields);

						//UpdateOptions options = new UpdateOptions().upsert(false);
						BasicDBObject id;
						BasicDBObject update;

						id = new BasicDBObject("_id",splitFields.get(0).get("_id"));
						update = new BasicDBObject("$set" , new BasicDBObject("mu",muRec2.toString()));
						table.updateOne(id,update);

						id = new BasicDBObject("_id",splitFields.get(1).get("_id"));
						update = new BasicDBObject("$set" , new BasicDBObject("mu",muRec1.toString()));
						table.updateOne(id,update);
							
						/*
                        ingresslog.update_one({"_id": oid[0]},{"$set": { "mu": mu[1]} }, upsert=False)
                        ingresslog.update_one({"_id": oid[1]},{"$set": { "mu": mu[0]} }, upsert=False)
						*/

						} else {
							System.out.println("swapping doesn't work");
						}

					} else {
						System.out.println("" + splitFields.size()+ " != 2 split fields");
					}
				}

/*
			if (timetime != (int)(System.nanoTime() / 1000000000.0))
			{
				timetime = (int)(System.nanoTime() /1000000000.0);
				int totaltime = timetime - starttime;
				double fps = count / totaltime;
				System.out.println("" + count + " ("+totaltime +") " + fps +" tps" );
			}
			count++;
*/
		}
	} catch (Exception e) {
	            System.out.println( " Error: " + e.toString() );
	}
}
}
