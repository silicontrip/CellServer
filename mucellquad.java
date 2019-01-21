import java.util.*;
import org.bson.*;
import com.google.common.geometry.*;
import com.mongodb.*;
import com.mongodb.client.*;

public class mucellquad {

        private static UniformDistribution getAveChildMU(S2CellId cell,CellServer cs)
        {
                if (cell.level() < 13)
                {
                    S2CellId id = cell.childBegin();
                        UniformDistribution ttmu = new UniformDistribution (0,0);
                    for (int pos = 0; pos < 4; ++pos, id = id.next())
                        {
                                UniformDistribution mu = getMU(id,cs);
                                if (mu == null)
                                        return null;
                                ttmu = ttmu.add(mu);
                        }

                    return ttmu.div(4.0);
                }
                return null;
        }
    private static UniformDistribution getMU(S2CellId cell,CellServer cs)
    {

        UniformDistribution cellmu = cs.getCell(cell);
        UniformDistribution childmu = getAveChildMU(cell,cs);

        if (childmu == null)
                return cellmu;
        if (cellmu == null)
                return childmu;

	cellmu.refine(childmu);

        return cellmu;
    }

public static void main(String[] args) {


	CellServer cs = new CellServer();

	HashMap<S2CellId,UniformDistribution> cells = cs.getAllCells();

	int total = cells.size();
	// loop through all cells
	int count = 0 ;
	int timetime = (int)(System.nanoTime() / 1000000000.0);
	for (S2CellId cell: cells.keySet())
	{
		UniformDistribution nud = getMU(cell,cs);
		cs.putMU(cell,nud);
		count++;
                        if (timetime != (int)(System.nanoTime() / 1000000000.0))
                        {
                                timetime = (int)(System.nanoTime() /1000000000.0);
                                System.out.println("" + count +"/"+total);
                        }
	}

}

}
