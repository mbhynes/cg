package himrod.cg

import himrod.block._

import org.apache.spark.Logging

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

class ConjGradException(msg: String) extends Exception

class ConjGrad extends Serializable with Logging
{
}

object ConjGrad 
{
	private type ConjGradSoln = (BlockVec,Double,Int);

	// run the CG algorithm to solve A*x = b with initial guess x0
	def run(sc: SparkContext, 
		A: BlockMat,
		b: BlockVec,
		x0: BlockVec,
		tol: Double,
		maxIters: Int): ConjGradSoln =
	{
		logInfo("Generating initial residual, r = b - A*x0");
		val resid: BlockVec = b - A.multiply(x0);

		logInfo("Allocating ConjGradState");
		var sys: ConjGradState = ConjGradState(A,x0,resid,resid);

		var k = 0;
		var norm = sys.residNorm();

		logInfo("Starting CG iteration");
		logInfo("Iteration, Residual Norm");
		logInfo(k + "," + norm);

		while ((norm > tol) && (k < maxIters)) {
			k += 1;
			sys = sys.iterate();
			norm = sys.residNorm();
			logInfo(k + "," + norm);
		}
		(sys.x,norm,k);
	}

	// run the CG algorithm, but load A,b from textfile. Use x0 = 0 as initial guess
	def runFromFile(sc: SparkContext, 
		fmat: String, 
		fvec: String, 
		delim: String,
		matSize: BlockSize,
		bsize: BlockSize,
		tol: Double, 
		maxIters: Int): ConjGradSoln =
	{
		val vsize: Long = matSize.nrows; //vector length
		val vec_bsize: Long = bsize.ncols; //inner dimensions equal

		logInfo("Loading A from file " + fmat);
		val A: BlockMat = BlockMat.fromTextFile(sc,fmat,delim,matSize,bsize);

		logInfo("Loading A from file " + fvec);
		val b: BlockVec = BlockVec.fromTextFile(sc,fvec,delim,vsize,vec_bsize);

		logInfo("Initializing all-zero x0");
		val x0: BlockVec = BlockVec.zeros(sc,vsize,vec_bsize);

		ConjGrad.run(sc,A,b,x0,tol,maxIters);
	}

	// main method for spark_submit
	//
	// ./spark_submit this.jar -t tol -k max_it -A "mat_file" -b "vec_file"
	//
	private type ArgMap = Map[Symbol,String]

	private def next_arg(map: ArgMap, list: List[String]) : ArgMap = 
	{
		list match 
		{
			case Nil => map
			case ("--tol" | "-t") :: value :: tail =>
				next_arg(map ++ Map('tol -> value), tail)
			case ("--maxit" | "-k") :: value :: tail =>
				next_arg(map ++ Map('k -> value), tail)
			case ("--mat" | "-A") :: value :: tail =>
				next_arg(map ++ Map('A -> value), tail)
			case ("--sol" | "-b") :: value :: tail =>
				next_arg(map ++ Map('b -> value), tail)
			case ("--x0" | "-x") :: value :: tail =>
				next_arg(map ++ Map('x -> value), tail)
			case ("--delim" | "-d") :: value :: tail =>
				next_arg(map ++ Map('d -> value), tail)
			case ("--size" | "-n") :: value :: tail =>
				next_arg(map ++ Map('msize -> value), tail)
			case ("--bsize" | "-b") :: value :: tail =>
				next_arg(map ++ Map('bsize -> value), tail)
			case _ :: tail =>
				next_arg(map, tail)
		}
	}
	private def parseBlock(str: String, delim: String): BlockSize = 
	{
		val tokens: Array[String] = str.split(delim);
		BlockSize(tokens(0).toLong,tokens(1).toLong);
	}

	def main(args: Array[String]) = 
	{
		val conf = new SparkConf()
		val sc = new SparkContext(conf)

		val arg_list = args.toList;
		val vars: ArgMap = next_arg(Map(),arg_list);
		val fout = arg_list.last;

		val delim: String = {
			if (vars.contains('d))
				vars('d)
			else
				","
		}
		val matSize: BlockSize = {
			if (vars.contains('n))
				parseBlock(vars('n),delim);
			else
				throw new ConjGradException("MatSize required; specify: -n rows,cols ");
		}
		val bsize: BlockSize = {
			if (vars.contains('b))
				parseBlock(vars('b),delim);
			else
				throw new ConjGradException("BlockSize required; specify: -b rows,cols ");
		}
		val fin_mat: String = {
			if (vars.contains('A))
				vars('A);
			else
				"";
		}
		val fin_vec: String = {
			if (vars.contains('b))
				vars('b);
			else
				"";
		}
		val fin_x0: String = {
			if (vars.contains('x))
				vars('x);
			else
				"";
		}
		val maxIters: Int = {
			if (vars.contains('k))
				vars('k).toInt;
			else
				100;
		}
		val tol: Double = {
			if (vars.contains('tol))
				vars('k).toDouble;
			else
				1.0E-3;
		}
		
		val soln: ConjGradSoln = 
		{
			if (fin_vec.isEmpty || fin_mat.isEmpty)
			{ 
				logInfo("Generating random SPD matrix");
				val B: BlockMat = BlockMat.rand(sc,matSize,bsize) * 10;
				val A: BlockMat = B.multiply(B.transpose);

				logInfo("Generating random b vector");
				val b: BlockVec = BlockVec.rand(sc,matSize.ncols,bsize.ncols);

				val x0: BlockVec = {
					if (fin_x0.isEmpty)
					{
						logInfo("Generating all-zero x0");
						BlockVec.zeros(sc,matSize.ncols,bsize.ncols);
					}
					else
					{
						logInfo("Reading x0 from file: " + fin_x0);
						BlockVec.fromFile(sc,fin_x0,delim,matSize.ncols,bsize.ncols);
					}
				}

				run(sc,
					A,
					b,
					x0,
					tol,
					maxIters);
			}
			else
			{
				runFromFile(sc,
					fin_mat,
					fin_vec,
					delim,
					matSize,
					bsize,
					tol,
					maxIters);
			}
		}
		logInfo("Saving solution to file:" + fout);
		soln._1.saveAsTextFile(fout);
	}
}
