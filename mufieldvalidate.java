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

			// get me the field
                        Document data = (Document) entitycontent.get("data");
                        ArrayList<Document> capturedRegion = (ArrayList<Document>) data.get("points");

			S2Polygon thisField = cs.getS2Field(capturedRegion);
			// show me the cells
			//response = cs.cellalize(thisField);	
			// show me the mu

			UniformDistribution fieldmu = cs.muForField(thisField);

			// validate the score
			if (fieldmu.getUpper() > 0)
				if (!fieldmu.roundAboveZero().contains(score))
				{
					System.out.println( "score: " + score + " -> " + fieldmu + " : [" + doctodt(capturedRegion)  + "]");
					System.out.println("" + entitycontent);
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
