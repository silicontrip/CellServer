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

	private static String toDt(S2Polygon f)
	{
		JSONArray latLngs = new JSONArray();
		for (int l=0; l<f.numLoops(); l++)
		{
			S2Loop pl = f.loop(l);
			for (int v=0; v< pl.numVertices(); v++)
			{
				S2LatLng p = new S2LatLng(pl.vertex(v));
				JSONObject point = new JSONObject();
				point.put("lat",p.latDegrees());
				point.put("lng",p.lngDegrees());
				latLngs.put(point);
			}
		}
	
		JSONObject dt = new JSONObject();
		dt.put("type","polygon");
		dt.put("latLngs",latLngs);
		dt.put("color","#a24ac3");

		return dt.toString();
			

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

	protected boolean validateCell(HashMap<S2CellId,UniformDistribution> multi,S2CellId cell)
        {
		if (multi.get(cell) == null)
			return true; // or should we synthesize one from the children?
                if (cell.level() < 13)
                {
			S2CellId id = cell.childBegin();
                        UniformDistribution ttmu = new UniformDistribution (0,0);
			for (int pos = 0; pos < 4; ++pos, id = id.next())
                        {
                                UniformDistribution mu = multi.get(id);
                                if (mu == null)
                                        return true; // not enough info to validate
                                ttmu = ttmu.add(mu);
                        }

			try {
				ttmu.div(4.0).refine(multi.get(cell));
				return true;
			} catch (Exception e) {
				return false;
			}
                }
                return true;  // we don't have children of level 13 cells
        }

	protected boolean validateField (HashMap<S2CellId,UniformDistribution> multi,S2Polygon f, int score)
	{
		UniformDistribution fieldmu = muForField(multi,f); // need to use internal method.
		if (fieldmu.getUpper() == 0) // not known if valid.
			return true;
		return fieldmu.roundAboveZero().contains(score);
	}
	
	protected HashMap<S2CellId,UniformDistribution> processCells (HashMap<S2CellId,UniformDistribution> multi)
	{
		HashMap<S2CellId,UniformDistribution> multi2 = new HashMap<S2CellId,UniformDistribution>();

		// parent = (child[1] + child[2] + child[3] + child[4])/4

                // copy multiSet over
                for (S2CellId cell: multi.keySet())
                        multi2.put(cell, multi.get(cell));

		for (S2CellId cell: multi.keySet())
		{
			S2CellId parent = cell.parent();
			UniformDistribution parentMu = multi.get(parent);
			if (parentMu != null)
			{
			//	System.out.println("Parent Cell: " + parent.toToken() + " mu: " + parentMu);	
				S2CellId siblingo = parent.childBegin();
				for (int opos =0; opos < 4; ++opos, siblingo = siblingo.next())
				{
					UniformDistribution totalMu = parentMu.mul(4);
			//		System.out.println("this Cell: " + siblingo.toToken() + " total MU: " + totalMu);
					S2CellId sibling = parent.childBegin();
					for (int pos = 0; pos < 4; ++pos, sibling = sibling.next())
					{
						if (!sibling.toToken().equals(siblingo.toToken()))
						{
							UniformDistribution cellMu = multi.get(sibling);
							if (cellMu==null)
								totalMu.setLower(0.0);
							else
								totalMu = totalMu.sub(cellMu);
							totalMu.clampLower(0.0);
				//			System.out.println("take away cell: " + sibling.toToken() + " mu: " + cellMu + " = " + totalMu);
						}

					}
					UniformDistribution nud = multi2.get(siblingo);
				//	System.out.println("Resulting mu: " + totalMu + " <-> " + nud);
					if (nud == null)
						nud = totalMu;
					else
						nud.refine(totalMu);
					multi2.put(siblingo,nud);
				}
			} else {
				S2CellId sibling = parent.childBegin();
				UniformDistribution totalMu= new UniformDistribution(0,0);
				for (int opos =0; opos < 4; ++opos, sibling = sibling.next())
				{
					UniformDistribution cellMu = multi.get(sibling);
					//System.out.println ("Adding cell: " + sibling.toToken() + " mu: " + cellMu);
					if (cellMu != null)
						totalMu = totalMu.add(cellMu);
					else
					{
						totalMu.setLower(0.0); 
						totalMu.setUpper(0.0);
						break;
					}

				}
				//System.out.println("set parent: " + parent.toToken() + " = " + totalMu);
				if (totalMu.getUpper() > 0)
					multi2.put(parent,totalMu);

			}

		}

		return multi2;

	}


	protected void processField (HashMap<S2CellId,UniformDistribution> multi, S2Polygon f, UniformDistribution mu)
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

			UniformDistribution cellomu = multi.get(cello);

			//update cell
			if (cellomu == null)
				cellomu = score;
			else 
			{
				try {
					//UniformDistribution oldcell = new UniformDistribution(cellomu);
					cellomu.refine(score);
				} catch (Exception e) {
					System.err.print(cello.toToken() + " ");
					System.err.println(e.getMessage() + " : [" + toDt(f) + "]");
				}
			}
			cellomu.clampLower(0.0);
			multi.put(cello,cellomu);

			if (!validateCell(multi,cello) || !validateCell(multi,cello.parent()))
			{
				System.out.println("cell parent/child conflict: " + cello + " ["  + f + "] mu: " + mu);
			}
		}
	}
	protected HashMap<S2CellId,UniformDistribution> processSingle() throws Exception
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
						if (!validateCell(multi2,cello) || !validateCell(multi2,cello.parent()))
						{
							System.out.println("cell parent/child conflict: " + cello + " ["  + doctodt(capturedRegion) + "] mu: "+entitycontent.get("mu"));
						}
					}	
				}
			}
			// validate field!
			if (watchField != null)
			{
				if (!validateField(multi2,watchField,watchFieldMu))
				{
					// the last field causes this field to become invalid
					System.out.println("invalidates watch field: " + watchFieldMu + " -> " + muForField(multi2,watchField) + " : ["  + doctodt(capturedRegion) + "] mu: "+entitycontent.get("mu"));
					// might need to quit at this point
					throw new Exception("Invalidates Watch Field");
				}
			}
		}
		return multi2;	
	}

	
	protected  HashMap<S2CellId,UniformDistribution> processMulti (HashMap<S2CellId,UniformDistribution> multi) throws Exception
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
					processField(multi2,thisField,score);
				}
			}
			// validate field!
			if (!(watchField == null))
			{
				if (!validateField(multi2,watchField,watchFieldMu))
				{
					// the last field causes this field to become invalid
					System.out.println("invalidates watch field: " + watchFieldMu + " -> " + muForField(multi2,watchField) + " : ["  + doctodt(capturedRegion) + "] mu: "+entitycontent.get("mu"));
					// might need to quit at this point
					throw new Exception("Invalidates Watch Field");
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

	public static int diffCount (HashMap<S2CellId,UniformDistribution> a, HashMap<S2CellId,UniformDistribution> b)
	{
		int count =0;
		for (S2CellId cell: b.keySet())
			if (!b.get(cell).equals(a.get(cell)))
				count++;
		return count;
	}

	public static boolean diffMU (HashMap<S2CellId,UniformDistribution> a, HashMap<S2CellId,UniformDistribution> b)
	{
		if (a.size() != b.size()) return true;
		for (S2CellId cell: b.keySet())
			if (!b.get(cell).equals(a.get(cell)))
			{
				//System.err.println(a.get(cell) + " -> " + b.get(cell));
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


			HashMap<S2CellId,UniformDistribution> multi = new HashMap<S2CellId,UniformDistribution> ();
			HashMap<S2CellId,UniformDistribution> multi2;
			HashMap<S2CellId,UniformDistribution> multi3;
			//boolean diff = true;
				
			System.err.println("single...");

			multi3 = mu.processSingle();
			System.err.println("" + multi3.size() + " cells calculated");
			System.err.println("process cells...");
			multi2 = mu.processCells(multi3);
			System.err.println("" + diffCount(multi3,multi2) + " cells updated.");

			while (diffMU(multi,multi2)) {
				multi = multi2;
				System.err.println("iterating...");
				multi3 = mu.processMulti(multi);
			System.err.println("" + multi3.size() + " total " + diffCount(multi,multi3) + " cells updated.");
				System.err.println("process cells...");
				multi2 = mu.processCells(multi3);
			System.err.println("" + multi2.size() + " total " + diffCount(multi3,multi2) + " cells updated.");

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
