import java.util.*;
import org.bson.*;
import org.json.*;
import com.google.common.geometry.*;
import com.mongodb.*;
import com.mongodb.client.*;

public class mucelliter { 

	private static HashMap<S2CellId,UniformDistribution> cellStats;
	private static final double EPSILON = 1e-10;
	private CellServer cs;
	private double range = 0.5;
	private MongoClient mongo;
	private MongoDatabase db;
	private MongoCursor<Document> cursor;
	private MongoCollection<org.bson.Document> table;
	private String watchCell;
	private S2Polygon watchField;
	private Integer watchFieldMu;

	// these need better names now that they are instance variables.
	//HashMap<S2CellId,UniformDistribution> multi; 
	//HashMap<S2CellId,UniformDistribution> multi2;

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

	protected HashSet<S2LatLng> fieldHash(ArrayList<Document> f)
	{
		HashSet<S2LatLng> fieldKey = new HashSet<S2LatLng>();
		// as long as there are 3
		for (Document l : f)
			fieldKey.add(cs.locationToS2 (l));

		return fieldKey;
		
	}

	protected UniformDistribution muScore(String i)
	{
		//determine upper and lower MU
		Integer score = new Integer(i);
		double upper = (score + range);
		double lower =0.0;
		if (score > 1) 
			lower = (score - range);

		return new UniformDistribution(lower,upper);
	}

	protected UniformDistribution muScore(Document ec)
	{
		return muScore((String)ec.get("mu"));
	}

	protected double getIntArea(S2CellId cello, S2Polygon thisField)
	{
		S2Polygon intPoly = new S2Polygon();
		S2Polygon cellPoly = cs.polyFromCell(new S2Cell(cello));
		intPoly.initToIntersection(thisField, cellPoly);
		return intPoly.getArea() * 6367 * 6367 ;
	}

	protected UniformDistribution muForField(HashMap<S2CellId,UniformDistribution> multi,S2Polygon f)
	{
		UniformDistribution totalScore = new UniformDistribution(0,0);
		S2CellUnion cells = cs.getCellsForField(f);
		for (S2CellId cell: cells)
		{
			double area = getIntArea(cell,f);
			UniformDistribution cellmu = multi.get(cell);  // have to make this an instance variable.
			if (cellmu == null)
				return new UniformDistribution(0,0);  // unknown sentinel
			totalScore = totalScore.add(cellmu.mul(area));
		}
		return totalScore;

	}

	protected boolean validateField (HashMap<S2CellId,UniformDistribution> multi,S2Polygon f, int score)
	{
		UniformDistribution fieldmu = muForField(multi,f); // need to use internal method.
		if (fieldmu.getUpper() == 0) // not known if valid.
			return true;
		return fieldmu.roundAboveZero().contains(score);
	}

	protected void processField (HashMap<S2CellId,UniformDistribution> multi , HashMap<S2CellId,UniformDistribution> multi2 ,S2Polygon f, UniformDistribution mu)
	{
		S2CellUnion cells = cs.getCellsForField (f);
		for (S2CellId cello: cells) {
			UniformDistribution score = new UniformDistribution(mu);
			for (S2CellId celli: cells) 
				if (!cello.toToken().equals(celli.toToken()))
				{
					double area = getIntArea(celli,f);
					UniformDistribution cellmu = multi.get(celli);

					if (cellmu != null)
						score = score.sub(cellmu.mul(area));
					else
						score.setLower(0.0);
				}	

			double area = getIntArea(cello,f);
			score= score.div(area);

			UniformDistribution cellomu = multi2.get(cello);

			//update cell
			if (cellomu == null)
				cellomu = score;
			else 
			{
				try {
					UniformDistribution oldcell = new UniformDistribution(cellomu);
					cellomu.refine(score);
				} catch (Exception e) {
					System.err.print(cello.toToken() + " ");
					System.err.println(e.getMessage() + " : [" + f + "]");
				}
			}
			cellomu.clampLower(0.0);
			multi2.put(cello,cellomu);
		}
	}
	protected HashMap<S2CellId,UniformDistribution> processSingle()
	{
		// java mongo API doesn't have rewind
		cursor = table.find().iterator();
		HashMap<S2CellId,UniformDistribution> multi2 = new HashMap<S2CellId,UniformDistribution>();
		HashSet<HashSet<S2LatLng>> fieldSet = new HashSet<HashSet<S2LatLng>>();

		while (cursor.hasNext()) {
			Document entitycontent = cursor.next();
			
			ArrayList<Document> capturedRegion = (ArrayList<Document>)((Document) entitycontent.get("data")).get("points");
			HashSet<S2LatLng> fieldKey = fieldHash(capturedRegion);
			if (!fieldSet.contains(fieldKey))	
			{
				fieldSet.add(fieldKey);

				// should validate that the same fields have the same MU
				S2Polygon thisField = cs.getS2Field(capturedRegion);
				S2CellUnion cells = cs.getCellsForField (thisField);
				if ( cells.size() == 1)
				{
					Double area = thisField.getArea() * 6367 * 6367 ;

					UniformDistribution score = muScore(entitycontent).div(area);
					//System.out.println("single score -> " + score);
					// only 1 cell, is there a simpler way of getting it?
					for (S2CellId cello: cells) {
						UniformDistribution cellomu = multi2.get(cello);
						if (cellomu == null)
						{
							cellomu = score;
						} else {
							//System.out.println("single refine -> " + cello);
							cellomu.refine(score);
						}
						multi2.put(cello,cellomu);
					}	
				}
			}
			// validate field!
			if (watchField != null)
			{
				if (!validateField(multi2,watchField,watchFieldMu))
				{
					// the last field causes this field to become invalid
					System.out.println("invalidates watch field: ["  + doctodt(capturedRegion) + "]");
					// might need to quit at this point
				}
			}
		}
		return multi2;	
	}

	
	protected  HashMap<S2CellId,UniformDistribution> processMulti (HashMap<S2CellId,UniformDistribution> multi)
	{
		cursor = table.find().iterator();

		HashMap<S2CellId,UniformDistribution> multi2 = new HashMap<S2CellId,UniformDistribution>();
		HashSet<HashSet<S2LatLng>> fieldSet = new HashSet<HashSet<S2LatLng>>();

		// copy multiSet over 
		for (S2CellId cell: multi.keySet())
			multi2.put(cell, multi.get(cell));

		while (cursor.hasNext()) {
			Document entitycontent = cursor.next();
			ArrayList<Document> capturedRegion = (ArrayList<Document>)((Document) entitycontent.get("data")).get("points");
			HashSet<S2LatLng> fieldKey = fieldHash(capturedRegion);
			if (!fieldSet.contains(fieldKey))	
			{
				fieldSet.add(fieldKey);
				S2Polygon thisField = cs.getS2Field(capturedRegion);
				S2CellUnion cells = cs.getCellsForField (thisField);

				if (cells.size() > 1)
				{
					UniformDistribution  score =  muScore(entitycontent);
					processField(multi,multi2,thisField,score);
				}
			}
			// validate field!
			if (!(watchField == null))
			{
				if (!validateField(multi2,watchField,watchFieldMu))
				{
					// the last field causes this field to become invalid
					System.out.println("invalidates watch field: ["  + doctodt(capturedRegion) + "]");
					// might need to quit at this point
				}
			}
		}
		return multi2;
	}

	public mucelliter () 
	{
		range = 0.5;
		cellStats = new HashMap<S2CellId,UniformDistribution>();
		watchCell = new String("");

		mongo = new MongoClient("localhost", 27017);
                db = mongo.getDatabase("ingressmu");
		table = db.getCollection("ingressmu");

		cs = new CellServer();
	}


	public void setWatchCell (String s) { watchCell = s; }
	public void setRange (double d) { range = d; }
	public void setWatchField (S2Polygon f) { watchField = f; }
	public void setWatchFieldMu (String s) { watchFieldMu = new Integer(s); }

	public static boolean diffMU (HashMap<S2CellId,UniformDistribution> a, HashMap<S2CellId,UniformDistribution> b)
	{
		if (a.size() != b.size()) return true;
		for (S2CellId cell: b.keySet())
			if (!b.get(cell).equals(a.get(cell)))
			{
				System.err.println(a.get(cell) + " -> " + b.get(cell));
				return true;
			}
		return false;
	}

	public void putMU(S2CellId cell, UniformDistribution mu) { cs.putMU(cell,mu); }
	public UniformDistribution getMU(S2CellId cell) { return cs.getMU(cell); }


	public static void main(String[] args) {

		mucelliter mu = new mucelliter();		
		Arguments ag = new Arguments(args);

		if (ag.hasOption("v"))
			mu.setWatchCell(ag.getOptionForKey("v"));

		if (ag.hasOption("f"))
		{
			JSONArray dt = new JSONArray(ag.getOptionForKey("f"));
			JSONObject dField = (JSONObject)dt.get(0);	
			mu.setWatchField (CellServer.getS2Field((JSONArray)dField.get("latLngs")));
			if (ag.hasOption("m"))
				mu.setWatchFieldMu (ag.getOptionForKey("m"));
			else 
				System.out.println("Watch field without MU requested");

		}	
			
		System.err.println("starting...");
		try {


			HashMap<S2CellId,UniformDistribution> multi = new HashMap<S2CellId,UniformDistribution>();
			HashMap<S2CellId,UniformDistribution> multi2 = new HashMap<S2CellId,UniformDistribution>();
			//boolean diff = true;
				
			System.err.println("single...");

			multi2 = mu.processSingle();

			while (diffMU(multi,multi2)) {
				multi = multi2;
				System.err.println("iterating...");
				multi2 = mu.processMulti(multi);

			}


			boolean first = true;

			for (S2CellId cell : multi.keySet()) 
			{
				UniformDistribution newmu = multi.get(cell);
				UniformDistribution oldmu = mu.getMU(cell);
				if (!newmu.equals(oldmu))
				{
					double imp = 1;
					if (oldmu != null)
						imp = oldmu.perror() / newmu.perror();

					if (imp > 1.1) { // should be configurable threshold
						System.out.print("" + cell.toToken() + ": "+ imp + " " + oldmu + " -> " + newmu);
						S2LatLng cellp = cell.toLatLng();
						System.out.printf(" https://www.ingress.com/intel?z=15&ll=%f,%f\n",cellp.latDegrees(),cellp.lngDegrees());
					}
					mu.putMU(cell, newmu);
				}
			}

		} catch (Exception e)  {

			System.out.print ("Exception: ");
			System.out.println(e.getMessage());
			e.printStackTrace();

		}

	}

}
