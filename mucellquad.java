import java.util.*;
import org.bson.*;
import com.google.common.geometry.*;
import com.mongodb.*;
import com.mongodb.client.*;

public class mucellquad {

	HashMap<S2CellId,UniformDistribution> cells;

        private UniformDistribution getAveChildMU(S2CellId cell)
        {
                if (cell.level() < 13)
                {
                    S2CellId id = cell.childBegin();
                        UniformDistribution ttmu = new UniformDistribution (0,0);
                    for (int pos = 0; pos < 4; ++pos, id = id.next())
                        {
                                UniformDistribution mu = cells.get(cell);
                                if (mu == null)
                                        return null;
                                ttmu = ttmu.add(mu);
                        }

                    return ttmu.div(4.0);
                }
                return null;
        }
    private UniformDistribution getMU(S2CellId cell)
    {

        UniformDistribution cellmu = cells.get(cell);
        UniformDistribution childmu = getAveChildMU(cell);

        if (childmu == null)
                return cellmu;
        if (cellmu == null)
                return childmu;

	cellmu.refine(childmu);

        return cellmu;
    }
	public void setCells(HashMap<S2CellId,UniformDistribution> c) { cells = c; }
	public void validate ()
	{
		int total = cells.size();
		int count = 0 ;
		int timetime = (int)(System.nanoTime() / 1000000000.0);

		for (S2CellId cell: cells.keySet())
		{
			UniformDistribution nud = getMU(cell);
			//System.out.println("" + cells.get(cell) + " <-> " + nud);
		//cs.putMU(cell,nud);
			cells.put(cell,nud);
		count++;
                        if (timetime != (int)(System.nanoTime() / 1000000000.0))
                        {
                                timetime = (int)(System.nanoTime() /1000000000.0);
                                System.out.println("" + count +"/"+total);
                        }
		}
	}

	public void storeMu(CellServer cs) {
		for (S2CellId cell: cells.keySet())
		{
			UniformDistribution nud = cells.get(cell);
			UniformDistribution oud = cs.getMU(cell);
			if (!nud.equals(oud))
			{
				System.out.println("" + oud + " -> " + nud);
				cs.putMU(cell,nud);
			}
		}
	}
	



public static void main(String[] args) {


	CellServer cs = new CellServer();

	mucellquad mu = new mucellquad();
	mu.setCells(cs.getAllCells());
	mu.validate();
	mu.storeMu(cs);


}

}
