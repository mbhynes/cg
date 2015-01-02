package himrod.cg

import himrod.block._

import org.apache.spark.Logging

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.storage.StorageLevel

class ConjGradException(msg: String) extends Exception

object ConjGrad extends Logging
{
	private def logStdout(msg: String) = 
	{
		val time: Long = System.currentTimeMillis/1000;
		logInfo(msg);
		println(time + ": " + msg);
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
			case ("--obj" | "-o") :: tail => {
				logStdout("ObjectFile flag set to true");
				next_arg(map ++ Map('loadObjFile -> "true"), tail);
			}
			case ("--tol" | "-t") :: value :: tail =>{
				logStdout("Running with tol = " + value.toDouble);
				next_arg(map ++ Map('tol -> value), tail);
			}
			case ("--maxit" | "-k") :: value :: tail => {
				logStdout("Running with " + value + " iterations");
				next_arg(map ++ Map('k -> value), tail);
			}
			case ("--mat" | "-A") :: value :: tail => {
				logStdout("Using matrix A from file: " + value);
				next_arg(map ++ Map('A -> value), tail);
			}
			case ("--sol" | "-b") :: value :: tail => {
				logStdout("Using vector b from file: " + value);
				next_arg(map ++ Map('b -> value), tail);
			}
			case ("--x0" | "-x") :: value :: tail => {
				logStdout("Using initial x0 from file: " + value);
				next_arg(map ++ Map('x -> value), tail);
			}
			case ("--delim" | "-d") :: value :: tail => {
				logStdout("Using delim: " + value);
				next_arg(map ++ Map('delim -> value), tail);
			}
			case ("--size" | "-ms") :: value :: tail => {
				logStdout("Using MatSize: " + value);
				next_arg(map ++ Map('msize -> value), tail);
			}
			case ("--bsize" | "-bs") :: value :: tail => {
				logStdout("Using BlockSize: " + value);
				next_arg(map ++ Map('bsize -> value), tail);
			}
			case other :: tail => {
				logStdout("Read non-flag value: " + other);
				next_arg(map, tail);
			}
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
			if (vars.contains('delim))
				vars('delim)
			else
				","
		}
		val matSize: BlockSize = {
			if (vars.contains('msize))
				parseBlock(vars('msize),delim);
			else
			{
				logError("MatSize required; specify: -n rows,cols ");
				throw new ConjGradException("MatSize required; specify: -n rows,cols ");
			}
		}
		val bsize: BlockSize = {
			if (vars.contains('bsize))
				parseBlock(vars('bsize),delim);
			else
			{
				logError("BlockSize required; specify: -b rows,cols ");
				throw new ConjGradException("BlockSize required; specify: -b rows,cols ");
			}
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
				vars('tol).toDouble;
			else
				1.0E-3;
		}
		
		val soln: ConjGradSoln = 
		{
			if (fin_vec.isEmpty || fin_mat.isEmpty)
			{ 
				logStdout("Generating random SPD matrix");
				val A: BlockMat = BlockMat.randSPD(sc,matSize,bsize);
				logStdout("Materializing A; A has "+ A.blocks.count + " elements");

				logStdout("Generating random b vector");
				val b: BlockVec = BlockVec.rand(sc,matSize.ncols,bsize.ncols);
				logStdout("Materializing b; b has "+ b.blocks.count + " elements");

				val x0: BlockVec = 
				{
					if (fin_x0.isEmpty)
					{
						logStdout("Generating all-zero x0");
						BlockVec.zeros(sc,matSize.ncols,bsize.ncols);
					}
					else
					{
						logStdout("Reading x0 from file: " + fin_x0);
						if (vars.contains('loadObjFile))
							BlockVec.fromObjectFile(sc,fin_x0,matSize.ncols,bsize.ncols);
						else
							BlockVec.fromTextFile(sc,fin_x0,delim,matSize.ncols,bsize.ncols);
					}
				}
				logStdout("Materializing x0; x0 has "+ x0.blocks.count + " elements");

				run(sc,
					A,
					b,
					x0,
					tol,
					maxIters);
			}
			else
			{
				if (vars.contains('loadObjFile))
					runFromObjectFile(sc,
						fin_mat,
						fin_vec,
						matSize,
						bsize,
						tol,
						maxIters);
				else
					runFromTextFile(sc,
						fin_mat,
						fin_vec,
						delim,
						matSize,
						bsize,
						tol,
						maxIters);
			}
		}

		logStdout("Saving solution to file:" + fout);

		if (vars.contains('loadObjFile))
			soln._1.saveAsTextFile(fout);
		else
			soln._1.saveAsObjectFile(fout);
	}

	private type ConjGradSoln = (BlockVec,Double,Int);
	private val blocking: Boolean = false;

	// run the CG algorithm to solve A*x = b with initial guess x0
	def run(sc: SparkContext, 
		A: BlockMat,
		b: BlockVec,
		x0: BlockVec,
		tol: Double,
		maxIters: Int): ConjGradSoln =
	{
		logStdout("Generating initial residual, r = b - A*x0");
		var resid: BlockVec = b - A.multiply(x0);

		var k: Int = 0;

		var norm_sqr: Double = BlockVec.normSquared(resid);
		var norm: Double = math.sqrt(norm_sqr);

		logStdout(k + "," + norm);

		// matrix-vector multiplication A*p
		var p: BlockVec = resid;
		var Ap: BlockVec = A.multiply(p);

		// update x,resid
		var alpha: Double = norm_sqr / (p.dot(Ap));
		var x: BlockVec = x0;
		var x_new: BlockVec = x + p*alpha;
		var resid_new: BlockVec = resid - (Ap)*alpha;

		// update search direction
		var norm_sqr_new: Double = BlockVec.normSquared(resid_new);
		/*var norm_new: Double = math.sqrt(norm_sqr_new);*/

		var beta: Double = norm_sqr_new / norm_sqr;
		var p_new: BlockVec = resid_new + p*beta;

		k += 1;
		norm = math.sqrt(norm_sqr_new);
		logStdout(k + "," + norm);
		while ((norm > tol) && (k < maxIters)) 
		{
			//free the RDD blocks of old vectors
			x.blocks.unpersist(blocking);
			p.blocks.unpersist(blocking);
			resid.blocks.unpersist(blocking);

			//update old variables
			x = x_new;
			p = p_new;
			resid = resid_new;
			norm_sqr = norm_sqr_new;

			//free the RDD blocks of now unused vectors
			/*x_new.blocks.unpersist(blocking);*/
			/*p_new.blocks.unpersist(blocking);*/
			/*resid_new.blocks.unpersist(blocking);*/

			Ap = A.multiply(p);

			// update x,resid
			alpha = norm_sqr / (p.dot(Ap));
			x_new = x + p*alpha;
			resid_new = resid - (Ap)*alpha;

			// update search direction
			norm_sqr_new = BlockVec.normSquared(resid_new);

			beta = norm_sqr_new / norm_sqr;
			p_new = resid_new + p*beta;

			k += 1;
			norm = math.sqrt(norm_sqr_new);
			logStdout(k + "," + norm);
		}
		(x_new,norm,k);
	}

	// run the CG algorithm, but load A,b from textfile. Use x0 = 0 as initial guess
	def runFromObjectFile(sc: SparkContext, 
		fmat: String, 
		fvec: String, 
		matSize: BlockSize,
		bsize: BlockSize,
		tol: Double, 
		maxIters: Int): ConjGradSoln =
	{
		val vsize: Long = matSize.nrows; //vector length
		val vec_bsize: Long = bsize.ncols; //inner dimensions equal

		logStdout("Loading A from file " + fmat);
		val A: BlockMat = BlockMat.fromObjectFile(sc,fmat,matSize,bsize);
		logStdout("Materializing A: A has " + A.blocks.count + " elements");

		logStdout("Loading b from file " + fvec);
		val b: BlockVec = BlockVec.fromObjectFile(sc,fvec,vsize,vec_bsize);
		logStdout("Materializing b: b has " + b.blocks.count + " elements");

		logStdout("Initializing all-zero x0");
		val x0: BlockVec = BlockVec.zeros(sc,vsize,vec_bsize);
		logStdout("Materializing x0: x0 has " + x0.blocks.count + " elements");

		ConjGrad.run(sc,A,b,x0,tol,maxIters);
	}

	def runFromTextFile(sc: SparkContext, 
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

		logStdout("Loading A from file " + fmat);
		val A: BlockMat = BlockMat.fromTextFile(sc,fmat,delim,matSize,bsize);
		logStdout("Materializing A: A has" + A.blocks.count + " elements");

		logStdout("Loading b from file " + fvec);
		val b: BlockVec = BlockVec.fromTextFile(sc,fvec,delim,vsize,vec_bsize);
		logStdout("Materializing b: b has" + b.blocks.count + " elements");

		logStdout("Initializing all-zero x0");
		val x0: BlockVec = BlockVec.zeros(sc,vsize,vec_bsize);
		logStdout("Materializing x0: x0 has" + x0.blocks.count + " elements");

		ConjGrad.run(sc,A,b,x0,tol,maxIters);
	}
}

// will probably delete this... doesn't seem necessary.
/*case class ConjGrad(var sys: ConjGradState) extends Serializable with Logging*/
/*{*/
/*	type ConjGradSoln = (BlockVec,Double,Int);*/
/**/
/*	def solve(tol: Double, maxIters: Int): ConjGradSoln =*/
/*	{*/
/*		var k = 0;*/
/*		var norm = sys.residNorm();*/
/**/
/*		logStdout("Starting CG iteration");*/
/*		logStdout("Iteration, Residual Norm");*/
/*		logStdout(k + "," + norm);*/
/**/
/*		while ((norm > tol) && (k < maxIters)) {*/
/*			k += 1;*/
/*			sys = sys.iterate();*/
/*			norm = sys.residNorm();*/
/*			logStdout(k + "," + norm);*/
/*		}*/
/*		(sys.x,norm,k);*/
/*	}*/
/*}*/

	// run the CG algorithm to solve A*x = b with initial guess x0
	/*def run(sc: SparkContext, */
	/*	A: BlockMat,*/
	/*	b: BlockVec,*/
	/*	x0: BlockVec,*/
	/*	tol: Double,*/
	/*	maxIters: Int): ConjGradSoln =*/
	/*{*/
	/*	logStdout("Generating initial residual, r = b - A*x0");*/
	/*	val resid: BlockVec = b - A.multiply(x0);*/

	/*	logStdout("Allocating ConjGradState");*/
	/*	/*var sys: ConjGradState = ConjGradState(A,x0,resid,resid);*/*/

	/*	/*val sys: ConjGradState = ConjGradState(A,x0,resid,resid);*/*/
	/*	/*ConjGrad(sys).solve(tol,maxIters);*/*/

	/*	logStdout("Computing initial norm");*/
	/*	var k = 0;*/
	/*	var norm = sys.residNorm();*/

	/*	logStdout("Starting CG iteration");*/
	/*	logStdout("Iteration, Residual Norm");*/
	/*	logStdout(k + "," + norm);*/

	/*	while ((norm > tol) && (k < maxIters)) */
	/*	{*/
	/*		k += 1;*/
	/*		sys = sys.iterate();*/
	/*		norm = sys.residNorm();*/
	/*		logStdout(k + "," + norm);*/
	/*	}*/
	/*	(sys.x,norm,k);*/
	/*}*/
