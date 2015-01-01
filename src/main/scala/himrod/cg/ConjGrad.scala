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
			case ("--tol" | "-t") :: value :: tail =>{
				logInfo("Running with tol = " + value);
				next_arg(map ++ Map('tol -> value), tail);
			}
			case ("--maxit" | "-k") :: value :: tail => {
				logInfo("Running with " + value + " iterations");
				next_arg(map ++ Map('k -> value), tail);
			}
			case ("--mat" | "-A") :: value :: tail => {
				logInfo("Using matrix A from file: " + value);
				next_arg(map ++ Map('A -> value), tail);
			}
			case ("--sol" | "-b") :: value :: tail => {
				logInfo("Using vector b from file: " + value);
				next_arg(map ++ Map('b -> value), tail);
			}
			case ("--x0" | "-x") :: value :: tail => {
				logInfo("Using initial x0 from file: " + value);
				next_arg(map ++ Map('x -> value), tail);
			}
			case ("--delim" | "-d") :: value :: tail => {
				logInfo("Using delim: " + value);
				next_arg(map ++ Map('delim -> value), tail);
			}
			case ("--size" | "-ms") :: value :: tail => {
				logInfo("Using MatSize: " + value);
				next_arg(map ++ Map('msize -> value), tail);
			}
			case ("--bsize" | "-bs") :: value :: tail => {
				logInfo("Using BlockSize: " + value);
				next_arg(map ++ Map('bsize -> value), tail);
			}
			case other :: tail => {
				logInfo("Read non-flag value: " + other);
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
				vars('k).toDouble;
			else
				1.0E-3;
		}
		
		val soln: ConjGradSoln = 
		{
			if (fin_vec.isEmpty || fin_mat.isEmpty)
			{ 
				logInfo("Generating random SPD matrix");
				val A: BlockMat = BlockMat.randSPD(sc,matSize,bsize);
				/*val A: BlockMat = BlockMat.rand(sc,matSize,bsize);*/
				logInfo("Materializing A; A has "+ A.blocks.count + "elements");
				/*A.saveAsTextFile("A");*/
				/*logInfo("Done writing A");*/

				logInfo("Generating random b vector");
				val b: BlockVec = BlockVec.rand(sc,matSize.ncols,bsize.ncols);
				logInfo("Materializing b; b has "+ b.blocks.count + "elements");
				/*logInfo("Writing b to file: b");*/
				/*b.saveAsTextFile("b");*/
				/*logInfo("Done writing b");*/

				val x0: BlockVec = 
				{
					if (fin_x0.isEmpty)
					{
						logInfo("Generating all-zero x0");
						BlockVec.zeros(sc,matSize.ncols,bsize.ncols);
					}
					else
					{
						logInfo("Reading x0 from file: " + fin_x0);
						BlockVec.fromTextFile(sc,fin_x0,delim,matSize.ncols,bsize.ncols);
					}
				}
				logInfo("Materializing x0; x0 has "+ x0.blocks.count + "elements");

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

	type ConjGradSoln = (BlockVec,Double,Int);

	// run the CG algorithm to solve A*x = b with initial guess x0
	def run(sc: SparkContext, 
		A: BlockMat,
		b: BlockVec,
		x0: BlockVec,
		tol: Double,
		maxIters: Int): ConjGradSoln =
	{
		logInfo("Generating initial residual, r = b - A*x0");
		var resid: BlockVec = b - A.multiply(x0);

		var k: Int = 0;

		logInfo("Computing initial norm");
		var norm_sqr: Double = BlockVec.normSquared(resid);
		var norm: Double = math.sqrt(norm_sqr);

		logInfo("Starting CG iteration");
		logInfo("Iteration, Residual Norm");
		logInfo(k + "," + norm);

		// matrix-vector multiplication A*p
		var p: BlockVec = resid;
		logInfo("Computing A*p_k---start");
		var Ap: BlockVec = A.multiply(p);
		logInfo("Computing A*p_k---finish");

		// update x,resid
		logInfo("Computing alpha");
		var alpha: Double = norm_sqr / (p.dot(Ap));
		var x: BlockVec = x0;
		logInfo("Computing x_new");
		var x_new: BlockVec = x + p*alpha;
		logInfo("Computing resid_new");
		var resid_new: BlockVec = resid - (Ap)*alpha;

		// update search direction
		var norm_sqr_new: Double = BlockVec.normSquared(resid_new);
		var norm_new: Double = math.sqrt(norm_sqr_new);

		var beta: Double = norm_sqr_new / norm_sqr;
		logInfo("Computing p_new");
		var p_new: BlockVec = resid_new + p*beta;

		k += 1;
		logInfo(k + "," + norm);
		while ((norm_new > tol) && (k < maxIters)) 
		{
			x = x_new;
			p = p_new;
			resid = resid_new;
			norm = norm_new;
			norm_sqr = norm_sqr_new;

			logInfo("matrix-vector multiplication A*p");
			Ap = A.multiply(p);

			// update x,resid
			logInfo("Updating x,resid");
			alpha = norm_sqr / (p.dot(Ap));
			x_new = x + p*alpha;
			resid_new = resid - (Ap)*alpha;

			// update search direction
			logInfo("Computing new norm");
			norm_sqr_new = BlockVec.normSquared(resid_new);
			norm_new = math.sqrt(norm_sqr_new);

			logInfo("Computing p_k");
			beta = norm_sqr_new / norm_sqr;
			p_new = resid_new + p*beta;

			k += 1;

				/*norm = sys.residNorm();*/
			logInfo(k + "," + norm);
		}
		(x_new,norm_new,k);
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
		logInfo("Materializing A: A has" + A.blocks.count + " elements");

		logInfo("Loading b from file " + fvec);
		val b: BlockVec = BlockVec.fromTextFile(sc,fvec,delim,vsize,vec_bsize);
		logInfo("Materializing b: b has" + b.blocks.count + " elements");

		logInfo("Initializing all-zero x0");
		val x0: BlockVec = BlockVec.zeros(sc,vsize,vec_bsize);
		logInfo("Materializing x0: x0 has" + x0.blocks.count + " elements");

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
/*		logInfo("Starting CG iteration");*/
/*		logInfo("Iteration, Residual Norm");*/
/*		logInfo(k + "," + norm);*/
/**/
/*		while ((norm > tol) && (k < maxIters)) {*/
/*			k += 1;*/
/*			sys = sys.iterate();*/
/*			norm = sys.residNorm();*/
/*			logInfo(k + "," + norm);*/
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
	/*	logInfo("Generating initial residual, r = b - A*x0");*/
	/*	val resid: BlockVec = b - A.multiply(x0);*/

	/*	logInfo("Allocating ConjGradState");*/
	/*	/*var sys: ConjGradState = ConjGradState(A,x0,resid,resid);*/*/

	/*	/*val sys: ConjGradState = ConjGradState(A,x0,resid,resid);*/*/
	/*	/*ConjGrad(sys).solve(tol,maxIters);*/*/

	/*	logInfo("Computing initial norm");*/
	/*	var k = 0;*/
	/*	var norm = sys.residNorm();*/

	/*	logInfo("Starting CG iteration");*/
	/*	logInfo("Iteration, Residual Norm");*/
	/*	logInfo(k + "," + norm);*/

	/*	while ((norm > tol) && (k < maxIters)) */
	/*	{*/
	/*		k += 1;*/
	/*		sys = sys.iterate();*/
	/*		norm = sys.residNorm();*/
	/*		logInfo(k + "," + norm);*/
	/*	}*/
	/*	(sys.x,norm,k);*/
	/*}*/
