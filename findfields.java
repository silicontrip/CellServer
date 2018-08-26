import java.io.BufferedReader;
import java.io.FileReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.ArrayList;

import org.json.*;
import com.google.common.geometry.*;

import org.bson.*;
import com.mongodb.*;
import com.mongodb.client.*;


public class findfields
{

        private static String doctodt(ArrayList<Document> points,String colour)
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

                return new String ("{\"type\":\"polygon\",\"color\":\"" + colour + "\",\"latLngs\":[" +
                                        "{\"lat\":" + latE6A/1000000.0 + ",\"lng\":"+lngE6A/1000000.0+"},"+
                                        "{\"lat\":" + latE6B/1000000.0 + ",\"lng\":"+lngE6B/1000000.0+"},"+
                                        "{\"lat\":" + latE6C/1000000.0 + ",\"lng\":"+lngE6C/1000000.0+"}"+
                                        "]}");

        }


    public static void main(String[] args)
    {

	MongoClient mongo;
	MongoDatabase db;
	MongoCursor<Document> cursor;
	MongoCollection<org.bson.Document> table;


        try
        {
		String watchCell = new String (args[0]); //yeah null pointer exception if wrong arguments specified!

                mongo = new MongoClient("localhost", 27017);
                db = mongo.getDatabase("ingressmu");
                table = db.getCollection("ingressmu");

		CellServer cs = new CellServer();


		cursor = table.find().iterator();

		Boolean first = true;
		System.out.print("[");

		while (cursor.hasNext()) {
                        Document entitycontent = cursor.next();

			Document data = (Document) entitycontent.get("data");
			ArrayList<Document> capturedRegion = (ArrayList<Document>) data.get("points");

                        Integer score;
                        try {
                                score =  new Integer((String)entitycontent.get("mu"));
                        } catch (ClassCastException e) {
                                score =  (Integer)entitycontent.get("mu");
                        }

			S2Polygon thisField = cs.getS2Field(capturedRegion);
			S2CellUnion cells = cs.getCellsForField(thisField);
			//response =  cs.getIntersectionMU(cells,thisField);

			for (S2CellId cello: cells) {
				if (watchCell.equals(cello.toToken()))
				{
					double area = thisField.getArea() * 6367 * 6367 ;

					//System.out.println ("mu/km:" + score / area);
					// determine mu/km
					// convert to colour
					// print field 
					if (first)
						first = false;
					else
						System.out.print(",");
					System.out.print(doctodt(capturedRegion,"#8040c0"));

				}
			}

		}
		System.out.println("]");
        }
        catch ( Exception e )
        {
            System.out.println( " Error: " + e.toString() );
        }
    }
    
}

