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

public class showcell
{

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

		for(String data: args) {
			JSONArray dtobj = new JSONArray(data); 
                        //System.out.println( m_num + " JSON: " + dtobj );
                        if ( dtobj.length()==0 )
                        {
                            System.out.println( "No Fields." );
                            return;
                        }
                        else
                        {
				UniformDistribution totalmu = new UniformDistribution(0,0);
				for (int i=0; i < dtobj.length(); i++)
				{
					response = cs.cellalize(dtobj.getJSONObject(i));

					JSONArray points = dtobj.getJSONObject(i).getJSONArray("latLngs");

					BasicDBObject pointA = new BasicDBObject();
					BasicDBObject pointB = new BasicDBObject();
					BasicDBObject pointC = new BasicDBObject();
					long latE6,lngE6;
					latE6 =(long)(points.getJSONObject(0).getDouble("lat") * 1000000);
					lngE6 = (long)(points.getJSONObject(0).getDouble("lng") * 1000000);
					pointA.put("latE6", latE6);
					pointA.put("lngE6", lngE6);
					BasicDBObject elemA = new BasicDBObject();
					elemA.put("$elemMatch",pointA);
					pointB.put("latE6", (long)(points.getJSONObject(1).getDouble("lat") * 1000000.0));
					pointB.put("lngE6", (long)(points.getJSONObject(1).getDouble("lng") * 1000000.0));
					BasicDBObject elemB = new BasicDBObject();
					elemB.put("$elemMatch",pointB);
					pointC.put("latE6", (long)(points.getJSONObject(2).getDouble("lat") * 1000000.0));
					pointC.put("lngE6", (long)(points.getJSONObject(2).getDouble("lng") * 1000000.0));
					BasicDBObject elemC = new BasicDBObject();
					elemC.put("$elemMatch",pointC);

					BasicDBList pointlist = new BasicDBList();

					pointlist.add(elemA);   
					pointlist.add(elemB);   
					pointlist.add(elemC);   

					BasicDBObject allpoints = new BasicDBObject();
					allpoints.put("$all",pointlist);
                
					BasicDBObject findpoints = new BasicDBObject();
					findpoints.put("data.points",allpoints);

					//System.out.println(findpoints);
					System.out.println("found: " + table.count(findpoints));
					cursor = table.find(findpoints).iterator();


					while (cursor.hasNext()) {
						Document entitycontent = cursor.next();
						int mu = Integer.parseInt(entitycontent.getString("mu"));
						S2Polygon fd = cs.getS2Field( (ArrayList<Document>) ((Document) entitycontent.get("data")).get("points"));
						double area = fd.getArea() * 6367 * 6367;
						System.out.println(entitycontent);
						//System.out.println(fd);
						System.out.println("MU: " + mu + " Area: " + area + " mu/km: " + mu/area);
					}


		//	    System.out.println( "" + response.toString(4) + "\n\r" );
					UniformDistribution fieldmu = new UniformDistribution(0,0);
					double min_mu = 0;
					double max_mu = 0;
					double total_area = 0;
					for (Iterator<String> id= response.keys(); id.hasNext();)
					{
						String cellid = id.next();
						JSONObject cell = response.getJSONObject(cellid);
						total_area += cell.getDouble("area");
						//System.out.println(cell);
						if (cell.has("mu_min"))
						{
							UniformDistribution cellmu = new UniformDistribution(cell.getDouble("mu_min"),cell.getDouble("mu_max"));
							System.out.print("" + cellid + ": " + cellmu + " x " + cell.getDouble("area") + " = ");
							cellmu = cellmu.mul(cell.getDouble("area"));
							//double cmin_mu = cell.getDouble("area") * cell.getDouble("mu_min");
							//double cmax_mu = cell.getDouble("area") * cell.getDouble("mu_max");
							//System.out.print("field: " + fieldmu + " + ");
							System.out.println("" + cellmu);
							fieldmu = fieldmu.add(cellmu);
							//System.out.println(" =  " + fieldmu);
							//min_mu += cmin_mu;
							//max_mu += cmax_mu;
						} else {
							System.out.println("No Information for cell: " + cellid);
						}
					}
					System.out.println ("MU: "  + fieldmu+ " " + fieldmu.mean() + " +/- " + fieldmu.perror() * 100 + "% " + total_area + " km^2");
					System.out.println ("Real MU: "  + fieldmu.round());
					totalmu = totalmu.add(fieldmu.round());
				}
				System.out.println ("TotalMU: "  + totalmu+ " " + totalmu.mean() + " +/- " + totalmu.perror() * 100 + "%");
                        }
		}
        }
        catch ( Exception e )
        {
            System.out.println( " Error: " + e.toString() );
        }
    }
    
}

