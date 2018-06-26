import java.util.*;
import org.bson.*;
import com.google.common.geometry.*;
import com.mongodb.*;
import com.mongodb.client.*;

public class mucelliter { 

	private static HashMap<S2CellId,UniformDistribution> cellStats;
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

	protected static HashMap<S2CellId,UniformDistribution> process (MongoCursor<Document> cursor, HashMap<S2CellId,UniformDistribution> multi, boolean single,String watchCell)
	{
		double range = 0.5; // some how configurable (1.0 or 0.5)...

		HashMap<S2CellId,UniformDistribution> multi2 = new HashMap<S2CellId,UniformDistribution>();
		HashSet<HashSet<S2LatLng>> fieldSet = new HashSet<HashSet<S2LatLng>>();

		//CellServer cs = new CellServer();

		S2RegionCoverer rc = new S2RegionCoverer();
			// ingress mu calculation specifics
		rc.setMaxLevel(13);
		rc.setMinLevel(0);
		rc.setMaxCells(20);	

		for (S2CellId cell: multi.keySet())
		{
			if (watchCell.equals(cell.toToken()))
				System.out.println("CELL: " + multi.get(cell)); 
			multi2.put(cell, multi.get(cell));
		}

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

			HashSet<S2LatLng> fieldKey = new HashSet<S2LatLng>();

			fieldKey.add(latlngA);
			fieldKey.add(latlngB);
			fieldKey.add(latlngC);

			if (!fieldSet.contains(fieldKey))  { // do not process the same field twice
				S2Polygon thisField = pb.assemblePolygon();

				Double totalArea = thisField.getArea() * 6367 * 6367 ;
				HashSet<S2CellId> multiKey = new HashSet<S2CellId>();

				S2CellUnion cells = rc.getCovering (thisField);
				fieldSet.add(fieldKey);

				if ( (single && cells.size() == 1) || (!single && cells.size() > 1))
				{
					Double area;

					//determine upper and lower MU
					double mu1u = (score + range);
					double mu1l =0.0;
					if (score > 1) 
						 mu1l = (score - range);

				

					// loop through field cells
					for (S2CellId cello: cells) {
						StringBuilder errmsg = new StringBuilder();
						UniformDistribution mus = new UniformDistribution(mu1l,mu1u);
						boolean watchOn = false;
						if (watchCell.equals(cello.toToken()))
						{
							watchOn = true;


							System.out.println("analysing: " + entitycontent.get("_id") +" [" + doctodt(capturedRegion) + "]");
							ArrayList<Document> ent = (ArrayList<Document>) entitycontent.get("ent"); 
							String cdate = new String("" + ent.get(1)); Date creation= new Date(Long.parseLong(cdate));
							System.out.println("ts = " +  ent.get(1) + " " + creation);
							System.out.println("" + cello.toToken() + " : " + mus  + " mu " + mus.div(totalArea) +" mu/km");

						}
					
						// loop through field cells
						for (S2CellId celli: cells) {
							// if not cell from outer loop
							if (!cello.toToken().equals(celli.toToken()))
							{
								S2Polygon intPoly = new S2Polygon();
								S2Polygon cellPoly = polyFromCell(new S2Cell(celli));
								intPoly.initToIntersection(thisField, cellPoly);
								area = intPoly.getArea() * 6367 * 6367 ;

								// subtract upper range * area from lower MU
								// subtract lower range * area from upper MU
							//	errmsg.append (mus +" ");
							//	errmsg.append (mus.div(totalArea) +" ");
								UniformDistribution cellmu = multi.get(celli);

								if (cellmu != null)
								{
									UniformDistribution cma = cellmu.mul(area);
									mus = mus.sub(cma);

									if (watchOn)
									{
									System.out.print( celli.toToken() );
									System.out.println(" - " + cma + " (" + cellmu + " x " + area + ") = " + mus);
									}

		
								}
								else
								{
									mus.setLower(0.0);
									if (watchOn)
										System.out.println("" + celli.toToken() + " undef = " + mus );
								}
								
							}	
						}
						S2Polygon intPoly = new S2Polygon();
						S2Polygon cellPoly = polyFromCell(new S2Cell(cello));
						intPoly.initToIntersection(thisField, cellPoly);
						area = intPoly.getArea() * 6367 * 6367 ;
						mus= mus.div(area);
						//errmsg.append(" " + mus + " ");
						UniformDistribution cellomu = multi2.get(cello);
						//if (ag.hasOption("v"))
/*
						if(watchOn)
						{
							System.out.println(cello.toToken() + " : " + cellomu + " x " +  mus);

						}
*/
						//lower_mu / outercell.area
						//upper_mu / outercell.area
						//update cell
						if (cellomu == null)
						{
							cellomu = mus;
							if (watchOn)
								System.err.println("NEW: " + cello.toToken() + " : " + cellomu);
						}
						else 
						{
							try {
								UniformDistribution oldcell = new UniformDistribution(cellomu);
								if (cellomu.refine(mus))
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
									System.out.println("" + cello.toToken() + " : " + mus  + " mu " + mus.div(totalArea) +" mu/km");
									System.out.println(cello.toToken() + " : " + oldcell + " x " +  mus);
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
		mongo = new MongoClient("localhost", 27017);
                db = mongo.getDatabase("ingressmu");
		table = db.getCollection("ingressmu");

		try {


			HashMap<S2CellId,UniformDistribution> multi = new HashMap<S2CellId,UniformDistribution>();
			HashMap<S2CellId,UniformDistribution> multi2;
			boolean diff = true;
				
			System.err.println("single...");
			cursor = table.find().iterator();
			multi = process(cursor,multi,true,watchCell);

			while (diff) {
				diff = false;
				System.err.println("iterating...");
				cursor = table.find().iterator();
				multi2 = process(cursor,multi,false,watchCell);
		
				for (S2CellId cell: multi2.keySet())
					if (!multi2.get(cell).equals(multi.get(cell)))
					{
						//System.err.println(multi.get(cell) + " -> " + multi2.get(cell));
						diff = true;
					}

				multi = multi2;
			}


			boolean first = true;

			CellServer cs = new CellServer();
			for (S2CellId cell : multi.keySet()) 
			{
				UniformDistribution newmu = multi.get(cell);
				UniformDistribution oldmu = cs.getMU(cell);
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
					cs.putMU(cell, newmu);
				}
			}

		} catch (Exception e)  {

			System.out.print ("Exception: ");
			System.out.println(e.getMessage());
			e.printStackTrace();

		}

	}

}
