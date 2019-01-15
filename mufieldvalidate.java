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

// loop all fields
		while (cursor.hasNext()) {
                        Document entitycontent = cursor.next();
                        // arg some of these are strings and some integers...
                        Integer score;
			// I should probably remove this, there is a support script to convert everything to a String
			// and then find out what's still writing integers

                        //try {
                              score =  new Integer((String)entitycontent.get("mu"));
                        //} catch (ClassCastException e) {
                        //        score =  (Integer)entitycontent.get("mu");
                        //}
			// get me the field
                        Document data = (Document) entitycontent.get("data");
                        ArrayList<Document> capturedRegion = (ArrayList<Document>) data.get("points");

			S2Polygon thisField = cs.getS2Field(capturedRegion);
			// show me the cells
			response = cs.cellalize(thisField);	

			UniformDistribution fieldmu = new UniformDistribution(0,0);

                        for (Iterator<String> id= response.keys(); id.hasNext();)
                        {
                                String cellid = id.next();
                                JSONObject cell = response.getJSONObject(cellid);
                                //System.out.println(cell);
				// do we have information for this cell
				// if not we should skip this whole thing
                                if (cell.has("mu_min"))
                                {
                                        UniformDistribution cellmu = new UniformDistribution(cell.getDouble("mu_min"),cell.getDouble("mu_max"));
                                        cellmu = cellmu.mul(cell.getDouble("area"));
                                        fieldmu = fieldmu.add(cellmu);
                                } else {
					break;
				}
                        }

			// validate the score
			if (!fieldmu.roundAboveZero().contains(score))
			{
				System.out.println ("score: " + score + " -> " + fieldmu);
			}

		}
	} catch (Exception e) {
	            System.out.println( " Error: " + e.toString() );
	}
}
}
