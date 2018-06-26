import java.io.BufferedReader;
import java.io.FileReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.json.*;
import com.google.common.geometry.*;
import org.bson.*;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.UpdateOptions;


public class CellServer
    implements Runnable
{
    private final Socket m_socket;
    private final int m_num;
    MongoClient m_mudb;
    BufferedReader m_in;
    OutputStreamWriter m_out;

	CellServer() { 
        m_socket = null; 
        m_num=0; 
        m_mudb = new MongoClient("localhost", 27017);

    } 

    CellServer( Socket socket, int num, MongoClient db) throws IOException
    {
        m_socket = socket;
        m_num = num;
        m_mudb = db;


        m_in = new BufferedReader( new InputStreamReader( m_socket.getInputStream() ) );
        m_out = new OutputStreamWriter( m_socket.getOutputStream() );

        Thread handler = new Thread( this, "handler-" + m_num );
        handler.start();
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

    private S2Polygon makePolyField (S2LatLng v1,S2LatLng v2,S2LatLng v3)
    {
        S2PolygonBuilder pb = new S2PolygonBuilder(S2PolygonBuilder.Options.UNDIRECTED_UNION);
        pb.addEdge(v1.toPoint(),v2.toPoint());
        pb.addEdge(v2.toPoint(),v3.toPoint());
        pb.addEdge(v3.toPoint(),v1.toPoint());
        return pb.assemblePolygon();
    }

    private static S2LatLng locationToS2 (Document loc) throws NumberFormatException
    {
        if (loc.containsKey("latE6") && loc.containsKey("lngE6")) {
            return S2LatLng.fromDegrees( loc.getInteger("latE6") / 1000000.0, loc.getInteger("lngE6")/1000000.0);
        }
        throw new NumberFormatException("Object doesn't contain lat/lng");
    }

    private static S2LatLng locationToS2 (JSONObject loc) throws NumberFormatException
    {
        if (loc.has("lat") && loc.has("lng")) {
            return S2LatLng.fromDegrees( loc.getDouble("lat"), loc.getDouble("lng"));
        }
        throw new NumberFormatException("Object doesn't contain lat/lng");
    }

    public S2Polygon getS2Field(ArrayList<Document> latlng)
    {
        return makePolyField(locationToS2((Document)latlng.get(0)),locationToS2((Document)latlng.get(1)),locationToS2((Document)latlng.get(2)));
    }
    
    public S2Polygon getS2Field(JSONArray latlng)
    {
        return makePolyField(locationToS2(latlng.getJSONObject(0)),locationToS2(latlng.getJSONObject(1)),locationToS2(latlng.getJSONObject(2)));
    }

    public BasicDBList makePointsIITC(JSONObject iitc_field)
    {
        JSONObject data = iitc_field.getJSONObject("data");
		JSONArray points = data.getJSONArray("points");
			
        BasicDBList pointlist = new BasicDBList();

        for (Object llobj: points)
        {
            BasicDBObject pts = new BasicDBObject();
            JSONObject latlng = (JSONObject)llobj;

            pts.put("latE6", latlng.getLong("latE6"));
	    	pts.put("lngE6", latlng.getLong("lngE6"));

			pointlist.add(new BasicDBObject("$elemMatch",pts));
        }
        return pointlist;
    }
    protected ArrayList<Document> findField(BasicDBList pointlist)
    {

	BasicDBObject findpoints = new BasicDBObject("data.points",new BasicDBObject("$all",pointlist));

		//BasicDBObject finddata = new BasicDBObject();
		//finddata.put("data",findpoints);

	MongoDatabase db = m_mudb.getDatabase("ingressmu");
	MongoCollection table = db.getCollection("ingressmu");
	MongoCursor<Document> cursor = table.find(findpoints).iterator();

		// is it a known field?
		// is the estimate error > +/- 0.5

        ArrayList<Document> fa = new ArrayList<Document>();

        while (cursor.hasNext())
            fa.add(cursor.next());

        return fa;
    }

    public S2CellUnion getCellsForField(S2Polygon thisField)
    {
        S2RegionCoverer rc = new S2RegionCoverer();
        // ingress mu calculation specifics
        rc.setMaxLevel(13);
        rc.setMinLevel(0);
        rc.setMaxCells(20);
        return rc.getCovering (thisField);
    }

	public void putMU(S2CellId cell, UniformDistribution mu)
	{
		MongoDatabase db;
		MongoCursor<Document> cursor;
		MongoCollection<org.bson.Document> table;
/*
query = {"cell": cell}         
muobj = {"cell": cell, "mu": celldata, "_id": cell}          
ingresslog.replace_one(query,muobj,upsert=True)
*/

		db = m_mudb.getDatabase("ingressmu");
		table = db.getCollection("mu");

		BasicDBObject findcell = new BasicDBObject("cell", cell.toToken());
		Document celldata = new Document("cell", cell.toToken());
		UpdateOptions options = new UpdateOptions();
		options.upsert(true);
		celldata.put("_id", cell.toToken());
		celldata.put("mu", mu.getArrayList());
		//table.updateOne(findcell, new BasicDBObject("$set", new BasicDBObject("mu", mu.getArrayList())));
		table.replaceOne(findcell,celldata,options);

	}

	public HashMap<S2CellId,UniformDistribution> getAllCells()
	{
		HashMap<S2CellId,UniformDistribution> cellStats = new HashMap<S2CellId,UniformDistribution>();
		//HashMap<String,UniformDistribution> cellStats = new HashMap<String,UniformDistribution>();
		MongoDatabase db;
                MongoCursor<Document> cursor;
                MongoCollection<org.bson.Document> table;

                db = m_mudb.getDatabase("ingressmu");
                table = db.getCollection("mu");	

		cursor= table.find().iterator();

		while(cursor.hasNext()) {
			Document cellmu = cursor.next();
			ArrayList<Double> lh = (ArrayList<Double>)cellmu.get("mu");
			UniformDistribution mu =  new UniformDistribution(lh.get(0),lh.get(1));
			//String id = new String ("0x" + (String)cellmu.get("cell"));
			S2CellId cell = S2CellId.fromToken((String)cellmu.get("cell"));
			cellStats.put(cell,mu);
			//cellStats.put((String)cellmu.get("cell"),mu);
		}
		return cellStats;
	}

    public UniformDistribution getCell(S2CellId cell)
	{
		MongoDatabase db;
		MongoCursor<Document> cursor;
		MongoCollection<org.bson.Document> table;

		db = m_mudb.getDatabase("ingressmu");
		table = db.getCollection("mu");
		BasicDBObject findcells = new BasicDBObject("cell", cell.toToken());

		cursor = table.find(findcells).iterator();

        //if (m_mudb.has(cell.toToken()))
		if(cursor.hasNext())
		{
		    Document cellmu = cursor.next();
		    ArrayList<Double> lh = (ArrayList<Double>)cellmu.get("mu");
		    return new UniformDistribution(lh.get(0),lh.get(1));
		}
		return null;
	}

    public UniformDistribution getMU(S2CellId cell)
    {

	UniformDistribution cellmu = getCell(cell);

	if (cellmu != null)
		return cellmu;
	else
        if (cell.level() < 13)
        {
            S2CellId id = cell.childBegin();
            UniformDistribution ttmu = new UniformDistribution (0,0);
            for (int pos = 0; pos < 4; ++pos, id = id.next())
		{
			UniformDistribution mu = getMU(id);
			if (mu == null)
				return null;
			ttmu = ttmu.add(getMU(id));
		}
            
            return ttmu.div(4.0);
        }
        return null;
    }

    public JSONObject getIntersectionMU(S2CellUnion cells,S2Polygon thisField)
    {
        JSONObject response = new JSONObject();
        for (S2CellId cell: cells)
        {
		//System.out.println("getIntersectionMU: " + cell.toToken());
            S2Polygon intPoly = new S2Polygon();
            S2Polygon cellPoly = polyFromCell(new S2Cell(cell));

            intPoly.initToIntersection(thisField, cellPoly);
            double area = intPoly.getArea() * 6367 * 6367 ;
            JSONObject cellinf  = new JSONObject();
            UniformDistribution mukm = getMU(cell);
            if (mukm != null)
		{
                cellinf.put("mu_min",mukm.getLower());
                cellinf.put("mu_max",mukm.getUpper());
		}

            cellinf.put("area",area);                            
            response.put(cell.toToken(),cellinf);
		}
        return response;

    }

	public JSONObject submitField(JSONObject iitcField)
	{
		JSONObject response = new JSONObject();
		double range = 0.5; // some how configurable (1.0 or 0.5)...

                HashMap<S2CellId,UniformDistribution> multi = new HashMap<S2CellId,UniformDistribution>();

                S2RegionCoverer rc = new S2RegionCoverer();
		// ingress mu calculation specifics
                rc.setMaxLevel(13);
                rc.setMinLevel(0);
                rc.setMaxCells(20);

		Integer score = new Integer(iitcField.getString("mu"));

		Document data = Document.parse (((JSONObject)iitcField.get("data")).toString());
		ArrayList<Document> capturedRegion = (ArrayList<Document>) data.get("points");

		S2Polygon thisField = getS2Field(capturedRegion);
		S2CellUnion cells = getCellsForField(thisField);

		Double area;

		//determine upper and lower MU
		double mu1u = (score + range);
		double mu1l =0.0;
		if (score > 1)
			mu1l = (score - range);

		for (S2CellId cello: cells) {
			UniformDistribution mus = new UniformDistribution(mu1l,mu1u);
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
					//      errmsg.append (mus +" ");
					//      errmsg.append (mus.div(totalArea) +" ");
					UniformDistribution cellmu = getMU(celli);

					if (cellmu != null)
					{
						UniformDistribution cma = cellmu.mul(area);
						mus = mus.sub(cma);
					}
					else
					{
						mus.setLower(0.0);
					}
				}
			}
			S2Polygon intPoly = new S2Polygon();
			S2Polygon cellPoly = polyFromCell(new S2Cell(cello));
			intPoly.initToIntersection(thisField, cellPoly);
			area = intPoly.getArea() * 6367 * 6367 ;
			mus= mus.div(area);

			UniformDistribution cellomu = getMU(cello);
			if (cellomu == null)
			{
				cellomu = mus;
			}
			else
			{
				try {
					cellomu.refine(mus);
				} catch (Exception e) {
					; // something something, out of range error
				}
			}
	
			cellomu.clampLower(0.0);
			multi.put(cello,cellomu);
		}
	
		for (S2CellId cell: multi.keySet())
		{
			response.put(cell.toToken(), multi.get(cell).getArrayList());
			putMU(cell, multi.get(cell));	
		}
		return response;
	}

    public JSONObject fieldUse (JSONObject iitcField)
    {
        BasicDBList point = makePointsIITC(iitcField);
        ArrayList<Document> foundList = findField(point);

        Integer known_mu = new Integer(-1);

        if (foundList.size() > 0)
        {
            // have converted all integers to strings.
            known_mu =  new Integer(foundList.get(0).getString("mu"));
//            known_mu =  (Integer)foundList.get(0).getInteger("mu");
        }

        Document data = Document.parse (((JSONObject)iitcField.get("data")).toString());
        ArrayList<Document> capturedRegion = (ArrayList<Document>) data.get("points");

        S2Polygon thisField = getS2Field(capturedRegion);
        S2CellUnion cells = getCellsForField(thisField);
	//System.out.println(cells);
        JSONObject mu =  getIntersectionMU(cells,thisField);
        double min_mu = 0;
        double max_mu = 0;
        boolean undefined = false;
        //System.out.println(mu);
        for (Iterator<String> id= mu.keys(); id.hasNext();)
        {
            String cellid = id.next();
            JSONObject cell = mu.getJSONObject(cellid);
            //System.out.println(cell);
            if (cell.has("mu_min"))
            {
                min_mu += cell.getDouble("area") * cell.getDouble("mu_min");
                max_mu += cell.getDouble("area") * cell.getDouble("mu_max");
            } else {
         //       System.out.println("undefined: " + cellid);
                undefined = true;
            }
        }
        // rounding?
        JSONObject response = new JSONObject();

        if (undefined)
        {
            min_mu=-1;
            max_mu=-1;
        }
        response.put("mu_min", min_mu);
        response.put("mu_max", max_mu);
        response.put("mu_known", known_mu);

        return response;

    }

    public JSONObject getMUForCells(JSONArray cellist)
    {
        MongoDatabase db;
        MongoCursor<Document> cursor;
        MongoCollection<org.bson.Document> table;

        db = m_mudb.getDatabase("ingressmu");
        table = db.getCollection("mu");

// probably need to unwrap json array
        BasicDBObject findcells = new BasicDBObject("cell", new BasicDBObject("$in",cellist));
        JSONObject jobj = new JSONObject();

        cursor = table.find(findcells).iterator();
        while (cursor.hasNext()) {
            Document cellmu = cursor.next();
            //System.out.println(cellmu);
            //cellmu.get("mu");
            ArrayList<Document> muArr = (ArrayList<Document>)cellmu.get("mu");
            JSONArray muJSON= new JSONArray(muArr);
            jobj.put(cellmu.getString("cell"),muJSON);
        }
        return jobj;
    }

	public JSONObject cellalize (JSONObject field)
	{
		if (field.has("latLngs"))
		{
			JSONArray latlng = field.getJSONArray("latLngs");
			if (latlng.length() == 3)
			{
            //System.out.println( m_num + " JSON: " + latlng );
                S2Polygon thisField = getS2Field(latlng);
                S2CellUnion cells = getCellsForField(thisField);
                // get mu for intersects cell

                return getIntersectionMU(cells,thisField);
			}
		}
		return new JSONObject();
	}

    protected void respondBadRequest (String message) throws IOException
    {
        String msg = new String("{\"status\": \"error\", \"message\": \""+message+"\"}");
        m_out.write("HTTP/1.1 400 Bad Request\r\n");
        m_out.write("Content-Length: " + msg.length() + "\r\n");
        m_out.write("Content-Type: application/json\r\n");
        m_out.write("\r\n");
        m_out.write(msg + "\r\n");

        m_out.flush();
    }

    protected void respondOK (String msg) throws IOException
    {
        m_out.write("HTTP/1.1 200 OK\r\n");
        m_out.write("Content-Length: " + msg.length() + "\r\n");
        m_out.write("Content-Type: application/json\r\n");
        m_out.write("\r\n");
        m_out.write(msg + "\r\n");

        m_out.flush();
    }

    public void run()
    {
        try
        {
            try
            {
                System.out.println( m_num + " Connected." );

                //out.write( "Welcome connection #" + m_num + "\n\r" );
                //out.flush();
                
                    JSONObject response = new JSONObject();

                    String line = new String(".");
                    ArrayList<String> request = new ArrayList<String>();

                    while (line.length() > 0)
                    {
                        line = m_in.readLine();
                        request.add(line);
                        //System.out.println("" + line.length() + " : " + line);
                    }
                    if (request.size() > 0)
                    {
                        line = request.get(0);
                        System.out.println(">>> " + line.length() + " : " + line);

                        String parts[]  = request.get(0).split(" ");
                        if (parts.length == 3)
                        {
                            //System.out.println(parts[1]);
                            String command[] = parts[1].split("/");
                            if (command.length==3)
                            {
                                line  = java.net.URLDecoder.decode(command[2]);
                        System.out.println(">>> " + line.length() + " : " + line);
                                JSONArray dtobj = new JSONArray(line); 
                                System.out.println( m_num + " JSON: " + dtobj );
                                if ( dtobj.length()==0 )
                                {
                                    System.out.println( m_num + " Closing Connection." );
                                    return;
                                }
                                else
                                {
                                    // GET CELLS FOR FIELD
                                    // GET MU FOR CELL
                                    // SUBMIT FIELD WITH MU
                                    // EXAMINE FIELDS FOR SUBMISSION
                                    System.out.println("Command:" + command[1]);
                                    if (command[1].equals("CELLS"))
                                    {
					                    response = cellalize(dtobj.getJSONObject(0));
                                        respondOK(""+response);
                                    }
                                    if (command[1].equals("MU"))
                                    {
                                        response = getMUForCells(dtobj.getJSONArray(0));
                                        respondOK(""+response);
                                    }
                                    if(command[1].equals("USE"))
                                    {
                                        response = fieldUse(dtobj.getJSONObject(0));
                                        respondOK(""+response);
                                        // find field   
                                        // get MU est
                                    }
					                if(command[1].equals("SUBMIT"))
					                {
						                response = submitField(dtobj.getJSONObject(0));
						                respondOK(""+response);
					                }
                                }
                              

                            } else {    
                                respondBadRequest("command length 3");
                                System.out.println( m_num + " Closed." );
                                return;
                            }
                        } else {
                            respondBadRequest("parts length 3");
                            System.out.println( m_num + " Closed." );
                            return;
                        }
                    } else {
                        respondBadRequest("request size 0");
                        System.out.println( m_num + " Closed." );
                        return;
                    }
            }
            finally
            {
                // respondBadRequest("finally");

                m_socket.close();
            }
        }
        catch ( IOException e )
        {
// an error occurs when we try to tell them that an error has occurred.  inception error.
            System.out.println( m_num + " Error: " + e.toString() );
        }
    }
    
    public static void main( String[] args )
        throws Exception
    {
        int port = 8080;
        if ( args.length > 0 )
        {
            port = Integer.parseInt( args[0] );
        }

        //BufferedReader reader = new BufferedReader(new FileReader("res50.json"));
        //String line = reader.readLine();
        //JSONObject mudb = new JSONObject(line);
        //reader.close();

        //System.out.println("JSON: " + mudb);
        System.out.println( "Accepting connections on port: " + port );

	MongoClient db = new MongoClient("localhost", 27017);

        int nextNum = 1;
        ServerSocket serverSocket = new ServerSocket( port );
        while ( true )
        {
            Socket socket = serverSocket.accept();
            CellServer hw = new CellServer( socket, nextNum++ ,db);
        }
    }
}

