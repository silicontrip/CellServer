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

	private static String dhex (double d)
	{
		int i = (int) (d * 255.0) ;
		if (i>255) i=255;
		if (i<0) i=0;
		return String.format("%02X",i);
	}
	
	private static String hsv2rgb(double h, double s, double v)
	{

		while (h<0.0) h += 360.0;
		while (h>360.0) h -= 360.0;
		double r,g,b;
		if (v < 0.0) 
		{
			r=0; g=0; b=0;
		} else if ( s < 0.0 ) {
			r=v; g=v; b=v;
		} else {
			double hf = h / 60.0;
			int i = (int)hf;
			double f = hf - i;
			double pv = v * (1.0 - s);
			double qv = v * (1.0 - s * f);
			double tv = v * (1.0 -s * (1.0-f));
//System.out.println("i: " + i + " hf: " + hf + " f: " + f + " pv: " + pv + " qv: " + qv + " tv: "+ tv);
			if (i==0||i==6) {
				r = v;
				g = tv;
				b = pv;
			} else if (i==1) {
				r = qv;
				g = v;
				b = pv;
			} else if (i==2) {
				r = pv;
				g = v;
				b = tv;
			} else if (i==3) {
				r=pv;
				g=qv;
				b=v;
			} else if (i==4) {
				r=tv;
				g=pv;
				b=v;
			} else if (i==5||i==-1) {
				r=v;
				g=pv;
				b=qv;
			} else {
				r=v; g=v; b=v;
			}
		}
		//System.out.println("R: " + r + " G: " + g + " B: " + b);
		return new String ("#" + dhex(r) + dhex(g) + dhex(b));
	}

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

		StringBuffer dtstr= new StringBuffer("[");


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

					String colour = hsv2rgb(score / area / 10.0,1.0,1.0);
					//System.out.println ("mu: " + score + " km:" + area + " mu/km:" + score / area);
					System.out.format ("%6d mu.  %10.3f km. %10.6f mu/km : " , score,area,score / area);
					System.out.println ("[" + doctodt(capturedRegion,colour) + "] ");
					// determine mu/km
					// convert to colour
					// print field 
					if (first)
						first = false;
					else
						dtstr.append(",");
					dtstr.append(doctodt(capturedRegion,colour));

				}
			}

		}
		dtstr.append("]");
		System.out.println("");
		System.out.println(dtstr);
        }
        catch ( Exception e )
        {
            System.out.println( " Error: " + e.toString() );
        }
    }
    
}

