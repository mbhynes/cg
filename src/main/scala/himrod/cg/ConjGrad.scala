package himrod.cg

import himrod.block._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object ConjGrad 
{
	/*val nc: Int = 800;*/
	/*val jobsPerCore: Int = 5;*/
	/*val maxIters = 30;*/

	private def computeMatBlockSize(numCores: Int, matSize: BlockSize, jobsPerCore: Int): BlockSize =
	{
		//use square matrices
		val N: Double = (matSize.nrows / math.sqrt(1.0 * jobsPerCore * numCores))
		val nrows: Long = (matSize.nrows / N).ceil.toLong;
		val ncols: Long = (matSize.nrows / N).ceil.toLong;
		BlockSize(nrows,ncols);
	}

	private def computeVecBlockSize(mat_bsize: BlockSize): Long = mat_bsize.ncols;


	def run(sc: SparkContext, 
		A: BlockMat,
		b: BlockVec,
		x0: BlockVec,
		tol: Double): (BlockVec,Double) =
	{
		val resid: BlockVec = b - A.multiply(x0);
		var sys: ConjGradState = ConjGradState(A,x0,resid,resid);

		var k = 1;
		while ((sys.residNorm() > tol) && (k < maxIters)) {
			k += 1;
			println("Current solution:");
			sys.x.print();
			sys = sys.iterate();
		}
		(sys.x,sys.residNorm());
	}


	def runFromFile(sc: SparkContext, 
		fmat: String, 
		fvec: String, 
		delim: String,
		tol: Double, 
		matSize: BlockSize): BlockVec =
	{
		val bsize: BlockSize = computeMatBlockSize(nc,matSize,jobsPerCore);
		val vsize: Long = computeVecBlockSize(bsize);
		val A: BlockMat = BlockMat.fromTextFile(sc,fmat,delim,matSize,bsize);
		val b: BlockVec = BlockVec.fromTextFile(sc,fvec,delim,matSize.nrows,vsize);
		val x0: BlockVec = BlockVec.zeros(sc,matSize.nrows,vsize);

		val resid: BlockVec = b - A.multiply(x0);
		var sys: ConjGradState = ConjGradState(A,x0,resid,resid);

		var k = 1;
		while ((sys.residNorm() > tol) && (k < maxIters)) {
			sys = sys.iterate;
			k = k+1;
		}
		sys.x;
	}
	/*def runFromFile(*/
	/*	sc: SparkContext, */
	/*	fmat: String, */
	/*	fvec: String, */
	/*	delim: String,*/
	/*	tol: Double): BlockVec = */
	/*{*/
	/*}*/

	/**/
	/*def run(*/
	/*	sc: SparkContext,*/
	/*	A: BlockMat, */
	/*	b: BlockVec, */
	/*	x: BlockVec, */
	/*	tol: Double): BlockVec = */
	/*{*/
	/*}*/
}
