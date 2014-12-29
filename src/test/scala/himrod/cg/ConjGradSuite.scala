package himrod.cg

import himrod.block._

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.log4j.Logger
import org.apache.log4j.Level

class ConjGradSuite extends FunSuite with LocalSparkContext
{
	Logger.getLogger("org").setLevel(Level.WARN);
	Logger.getLogger("akka").setLevel(Level.WARN);

	// files to read
	val N: Long = 16;
	val msize = BlockSize(N,N);
	val bsize = BlockSize(4,4);
	val vsize = N;
	val vec_bsize = 4;

	val vec_fin: String = "src/test/scala/himrod/cg/vec_data"
	val mat_fin: String = "src/test/scala/himrod/cg/mat_data"
	val delim: String = ",";

	val tol: Double = 0.0001;
	val maxIters: Int = 50;

	//need to write an SPD matrix to file before this will work.
	/*test("CG from file")*/
	/*{*/
	/*	val soln: (BlockVec,Double,Int) = ConjGrad.runFromFile(sc, */
	/*		mat_fin,*/
	/*		vec_fin,*/
	/*		delim,*/
	/*		msize,*/
	/*		bsize,*/
	/*		tol,*/
	/*		maxIters);*/

	/*	println("Final x after " + soln._3 + " iterations:");*/
	/*	soln._1.print();*/
	/*	println("resid:" + soln._2);*/

	/*	assert(soln._2 < tol);*/
	/*}*/

	test("CG with supplied vars")
	{
		// make a square SPD matrix and random vector x_real
		val B: BlockMat = BlockMat.eye(sc,msize,bsize)*10 + BlockMat.rand(sc,msize,bsize);
		val A: BlockMat = B.multiply(B.transpose);
		val x_real: BlockVec = BlockVec.rand(sc,vsize,vec_bsize);
		val x0: BlockVec = BlockVec.zeros(sc,vsize,vec_bsize);

		// generate b = A*x_real
		val b: BlockVec = A.multiply(x_real);

		val soln: (BlockVec,Double,Int) = ConjGrad.run(sc, 
			A,
			b,
			x0,
			tol,
			maxIters);

		println("Final x after " + soln._3 + " iterations:");
		soln._1.print();
		println("Actual x:");
		x_real.print();
		println("resid:" + soln._2);

		assert(soln._2 < tol);
	}
	
	def errorMessage(ex: Exception) =
	{
		println(ex);
		println(ex.getMessage);
		println(ex.getCause);
		println(ex.printStackTrace);
	}
}
