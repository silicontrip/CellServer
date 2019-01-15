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

		while (cursor.hasNext()) {
                        Document entitycontent = cursor.next();
                        // arg some of these are strings and some integers...
                        Integer score;
                        try {
                                score =  new Integer((String)entitycontent.get("mu"));
                        } catch (ClassCastException e) {
                                score =  (Integer)entitycontent.get("mu");
                        }
                        Document data = (Document) entitycontent.get("data");
                        ArrayList<Document> capturedRegion = (ArrayList<Document>) data.get("points");

                        Document vertexA = (Document) capturedRegion.get(0);
                        Document vertexB = (Document) capturedRegion.get(1);
                        Document vertexC = (Document) capturedRegion.get(2);
                                                        
                        S2LatLng latlngA = locationToS2 (vertexA);
                        S2LatLng latlngB = locationToS2 (vertexB);
                        S2LatLng latlngC = locationToS2 (vertexC);

                        S2PolygonBuilder pb = new S2PolygonBuilder(S2PolygonBuilder.Options.UNDIRECTED_UNION);
                        pb.addEdge(latlngA.toPoint(),latlngB.toPoint());
                        pb.addEdge(latlngB.toPoint(),latlngC.toPoint());
                        pb.addEdge(latlngC.toPoint(),latlngA.toPoint());	

			S2Polygon thisField = pb.assemblePolygon();
			response = cs.cellalize(thisField);	
			UniformDistribution fieldmu = new UniformDistribution(0,0);
                        double min_mu = 0;
                        double max_mu = 0;
                        for (Iterator<String> id= response.keys(); id.hasNext();)
                        {
                                String cellid = id.next();
                                JSONObject cell = response.getJSONObject(cellid);
                                //System.out.println(cell);
                                if (cell.has("mu_min"))
                                {
                                        UniformDistribution cellmu = new UniformDistribution(cell.getDouble("mu_min"),cell.getDouble("mu_max"));
                                        // System.out.print("" + cellid + ": " + cellmu + " x " + cell.getDouble("area") + " = ");
                                        cellmu = cellmu.mul(cell.getDouble("area"));
                                        //double cmin_mu = cell.getDouble("area") * cell.getDouble("mu_min");
                                        //double cmax_mu = cell.getDouble("area") * cell.getDouble("mu_max");
                                        //System.out.print("field: " + fieldmu + " + ");
                                        //System.out.println("" + cellmu);
                                        fieldmu = fieldmu.add(cellmu);
                                        System.out.println(" =  " + fieldmu);
                                        //min_mu += cmin_mu;
                                        //max_mu += cmax_mu;
                                }
                        }



		}
	} catch (Exception e) {
	            System.out.println( " Error: " + e.toString() );
	}
}
}
