import java.util.ArrayList;

public class UniformDistribution {

	double lower;
	double upper;

	public UniformDistribution (int a) { lower = a-0.5; upper = a+0.5; }
	public UniformDistribution (double a, double b) { 
		if (a<=b) {
			lower = a; 
			upper = b; 
		} else {
			lower = b; 
			upper = a; 
		} 
	}
	public UniformDistribution ( double a1, double a2, double b1, double b2) { 
		//System.out.println("NEW UD: " + a1 +","+a2+","+b1+","+b2);
		if (a1 <= a2 && a1 <= b1 && a1 <= b2) 
			lower = a1; 
		if (a2 <= a1 && a2 <= b1 && a2 <= b2) 
			lower = a2; 
		if (b1 <= a1 && b1 <= a2 && b1 <= b2) 
			lower = b1; 
		if (b2 <= a1 && b2 <= a2 && b2 <= b1)
			lower = b2; 

		if (a1 >= a2 && a1 >= b1 && a1 >= b2) 
			upper = a1; 
		if (a2 >= a1 && a2 >= b1 && a2 >= b2) 
			upper = a2; 
		if (b1 >= a1 && b1 >= a2 && b1 >= b2) 
			upper = b1; 
		if (b2 >= a1 && b2 >= a2 && b2 >= b1)
			upper = b2; 
	}
	public UniformDistribution(UniformDistribution ud)
	{
		this(ud.getLower(),ud.getUpper());
	}

	public void setUpper(double d) { upper = d; }
	public void setLower(double d) { lower = d; }

	public double getUpper() { return upper; }
	public double getLower() { return lower; }

	public void clampLower(double d) { if (lower < d) lower=d; }

	public double mean () { return (upper + lower) / 2.0; }
	public double error() { return (upper - lower); }
	public double perror() { return (upper - lower) / (upper + lower); }

	public ArrayList<Double> getArrayList()
	{
		ArrayList<Double> da = new ArrayList<Double>();
		da.add(new Double(getLower()));
		da.add(new Double(getUpper()));
		return da;
	}

	public UniformDistribution add (UniformDistribution a) { return new UniformDistribution(upper+a.getUpper(), upper+a.getLower(), lower+a.getLower(),lower+a.getUpper()); }
	public UniformDistribution mul (UniformDistribution a) { return new UniformDistribution(upper*a.getUpper(), upper*a.getLower(), lower*a.getLower(),lower*a.getUpper()); }

	public UniformDistribution sub (UniformDistribution a) { return new UniformDistribution(upper-a.getUpper(), upper-a.getLower(), lower-a.getLower(),lower-a.getUpper()); }
	public UniformDistribution div (UniformDistribution a) { return new UniformDistribution(upper/a.getUpper(), upper/a.getLower(), lower/a.getLower(),lower/a.getUpper()); }
	public UniformDistribution sub (double a) { return new UniformDistribution(upper-a, lower-a); }
	public UniformDistribution div (double a) { return new UniformDistribution(upper/a, lower/a); }
	public UniformDistribution mul (double a) { return new UniformDistribution(upper*a, lower*a); }

	public UniformDistribution abs () { 
		if (lower>=0 && upper>=0)
			return new UniformDistribution(lower,upper);
		if (lower<0 && upper <0)
			return new UniformDistribution(-lower,-upper);

		return new UniformDistribution(0,0,-lower,upper);

	}

	// anything else is false or uncertain.
	public boolean gt (UniformDistribution a) { return lower > a.getUpper(); }
	public boolean lt (UniformDistribution a) { return upper < a.getLower(); }
	public boolean ge (UniformDistribution a) { return lower >= a.getUpper(); }
	public boolean le (UniformDistribution a) { return upper <= a.getLower(); }
	public boolean equals (UniformDistribution a) { 
		if ( this == a ) return true;
		if ( !(a instanceof UniformDistribution) ) return false;

		return (lower == a.getLower() && upper == a.getUpper()); 
	}

	public boolean refine (UniformDistribution a) throws ArithmeticException { 

		// sanity check

		boolean changed = false;
		if ((upper > a.getUpper() || upper > a.getLower()) && ( lower < a.getLower() || lower < a.getUpper())) 
		{

			if (lower < a.getLower())
			{
				double d = a.getLower() - lower;
				//System.err.println("lower diff: " + d + "/" + lower);
				lower=a.getLower();
				changed=true;
			}

			if (upper > a.getUpper())
			{
				double d = upper - a.getUpper();
				//System.err.println("upper diff: " + d +  "/" + upper + " : " + lower);
				upper=a.getUpper();
				changed=true;
			}
		} else {
			throw new ArithmeticException( "REFINE Sanity check Exception : " + toString() + " x " + a);
		}
		return changed;

	}
	public String toString () { return "[" + lower + "," + upper + "]"; }
}
