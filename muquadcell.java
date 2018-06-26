import java.util.*;
import org.bson.*;
import com.google.common.geometry.*;
import com.mongodb.*;
import com.mongodb.client.*;

public class muquadcell { 

	private static HashMap<S2CellId,UniformDistribution> cellStats;
	//private static HashMap<String,UniformDistribution> cellStats;
	private static final double EPSILON = 1e-10;


	private static S2LatLng locationToS2 (Document loc) throws NumberFormatException
	{
		if (loc.containsKey("latE6") && loc.containsKey("lngE6")) {
			Integer latE6 = (Integer)loc.get("latE6");
			Integer lngE6 = (Integer)loc.get("lngE6");
			Double lat = new Double(latE6) / 1000000.0;
			Double lng = new Double(lngE6) / 1000000.0;

			return S2LatLng.fromDegrees(lat,lng);
		}

		throw new NumberFormatException("Object doesn't contain lat/lng");
	}

	private static S2Polygon polyFromCell (S2Cell cell) 
	{
		S2PolygonBuilder pb = new S2PolygonBuilder(S2PolygonBuilder.Options.UNDIRECTED_UNION);
		pb.addEdge(cell.getVertex(0),cell.getVertex(1));
		pb.addEdge(cell.getVertex(1),cell.getVertex(2));
		pb.addEdge(cell.getVertex(2),cell.getVertex(3));
		pb.addEdge(cell.getVertex(3),cell.getVertex(0));
		return pb.assemblePolygon();
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


	public static void main(String[] args) {

		double range = 0.5;
		MongoClient mongo;
		MongoDatabase db;
		MongoCursor<Document> cursor;
		MongoCollection<org.bson.Document> table;
		String watchCell = new String("");
		cellStats = new HashMap<S2CellId,UniformDistribution>();
		HashSet<HashSet<S2LatLng>> fieldSet = new HashSet<HashSet<S2LatLng>>();
		
		//HashMap<HashSet<S2CellId>,ArrayList<HashMap<S2CellId,UniformDistribution>>> multiCells = new HashMap<HashSet<S2CellId>,ArrayList<HashMap<S2CellId,UniformDistribution>>>();

		Arguments ag = new Arguments(args);

		if (ag.hasOption("v"))
			watchCell = ag.getOptionForKey("v");
			

		System.err.println("starting...");
		try {
		CellServer cs = new CellServer();
		cellStats = cs.getAllCells();

		Iterator it = cellStats.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry)it.next();
			System.out.println(pair.getKey() + " = " + pair.getValue() + " : " + ((UniformDistribution)pair.getValue()).perror());
		}

		} catch (Exception e)  {
			System.out.print ("Exception: ");
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
