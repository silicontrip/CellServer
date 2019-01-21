import java.util.*;
import org.bson.*;
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

		//Document vertexA = (Document) f.get(0);
		Document vertexB = (Document) f.get(1);
		Document vertexC = (Document) f.get(2);
							
		//S2LatLng latlngA = 
		fieldKey.add(cs.locationToS2 ((Document) f.get(0)));
		S2LatLng latlngB = cs.locationToS2 (vertexB);
		S2LatLng latlngC = cs.locationToS2 (vertexC);


		//fieldKey.add(latlngA);
		fieldKey.add(latlngB);
		fieldKey.add(latlngC);

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
					// only 1 cell, is there a simpler way of getting it?
					for (S2CellId cello: cells) {
						UniformDistribution cellomu = multi2.get(cello);
						if (cellomu == null)
						{
							cellomu = score;
						} else {
							cellomu.refine(score);
						}
						multi2.put(cello,cellomu);
					}	
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
		{
			if (watchCell.equals(cell.toToken()))
				System.out.println("CELL: " + multi.get(cell)); 
			multi2.put(cell, multi.get(cell));
		}

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
					Double totalArea = thisField.getArea() * 6367 * 6367 ; // only used in watch cell
					HashSet<S2CellId> multiKey = new HashSet<S2CellId>();

					UniformDistribution  score =  muScore(entitycontent);
					Double area;

					// loop through field cells
					for (S2CellId cello: cells) {
						StringBuilder errmsg = new StringBuilder();
						boolean watchOn = false;
						if (watchCell.equals(cello.toToken()))
						{
							watchOn = true;

							System.out.println("analysing: " + entitycontent.get("_id") +" [" + doctodt(capturedRegion) + "]");
							ArrayList<Document> ent = (ArrayList<Document>) entitycontent.get("ent"); 
							String cdate = new String("" + ent.get(1)); Date creation= new Date(Long.parseLong(cdate));
							System.out.println("ts = " +  ent.get(1) + " " + creation);
							System.out.println("" + cello.toToken() + " : " + score  + " mu " + score.div(totalArea) +" mu/km");

						}
					
						// loop through field cells
						for (S2CellId celli: cells) {
							// if not cell from outer loop
							if (!cello.toToken().equals(celli.toToken()))
							{
								area = getIntArea(celli,thisField);

								// subtract upper range * area from lower MU
								// subtract lower range * area from upper MU
							//	errmsg.append (score +" ");
							//	errmsg.append (score.div(totalArea) +" ");
								UniformDistribution cellmu = multi.get(celli);

								if (cellmu != null)
								{
									score = score.sub(cellmu.mul(area));
									if (watchOn)
									{
										System.out.print( celli.toToken() );
										System.out.println(" - (" + cellmu + " x " + area + ") = " + score);
									}
								}
								else
								{
									score.setLower(0.0);
									if (watchOn)
										System.out.println("" + celli.toToken() + " undef = " + score );
								}
							}	
						}

						area = getIntArea(cello,thisField);
						score= score.div(area);

						//errmsg.append(" " + score + " ");
						UniformDistribution cellomu = multi2.get(cello);
						//if (ag.hasOption("v"))
/*
						if(watchOn)
						{
							System.out.println(cello.toToken() + " : " + cellomu + " x " +  score);

						}
*/
						//lower_mu / outercell.area
						//upper_mu / outercell.area
						//update cell
						if (cellomu == null)
						{
							cellomu = score;
							if (watchOn)
								System.err.println("NEW: " + cello.toToken() + " : " + cellomu);
						}
						else 
						{
							try {
								UniformDistribution oldcell = new UniformDistribution(cellomu);
								if (cellomu.refine(score))
								{
									if (watchOn) 
										System.err.println("UPD: " + cello.toToken() + " : " + cellomu);
									;
								}
								if (watchOn && !oldcell.equals(cellomu))
								{
									System.out.println("analysing: " + entitycontent.get("_id") +" [" + doctodt(capturedRegion) + "]");
									ArrayList<Document> ent = (ArrayList<Document>) entitycontent.get("ent"); String cdate = new String("" + ent.get(1)); Date creation= new Date(Long.parseLong(cdate));
									System.out.println("ts = " +  ent.get(1) + " " + creation);
									System.out.println("" + cello.toToken() + " : " + score  + " mu " + score.div(totalArea) +" mu/km");
									System.out.println(cello.toToken() + " : " + oldcell + " x " +  score);
									System.out.println(" -> " + cellomu);
									System.out.println("");
								}
							} catch (Exception e) {
								ArrayList<Document> ent = (ArrayList<Document>) entitycontent.get("ent"); 
								String cdate = new String("" + ent.get(1)); 
								Date creation= new Date(Long.parseLong(cdate));
								if (watchOn)
								{
									System.out.print("" + totalArea + " "  + creation +" " );
									System.out.print(cello.toToken() + " ");
									System.out.println(e.getMessage() + " : [" + doctodt(capturedRegion) + "]");
								} else if (watchCell.length()==0) {
									System.out.print("" + totalArea + " "  + creation +" " );
									System.err.print(cello.toToken() + " ");
									System.err.println(e.getMessage() + " : [" + doctodt(capturedRegion) + "]");
								}
							}
						}
						//if (watchOn)
							//System.out.println("");
						cellomu.clampLower(0.0);
						multi2.put(cello,cellomu);
						//cs.putMU(cello,cellomu);

					}
					
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
