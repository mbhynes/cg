package himrod.cg

import himrod.block._

/*import org.apache.spark.rdd.RDD*/
/*import org.apache.spark.SparkContext*/
/*import org.apache.spark.SparkContext._*/

/*import org.apache.spark.SparkException*/
/*import org.apache.spark.storage.StorageLevel*/
/*import org.apache.spark.HashPartitioner*/
/*import org.apache.spark.mllib.random.RandomRDDs._*/

// A * x = b
case class ConjGradState(
	@transient A: BlockMat, //matrix
	x: BlockVec, //current x_k
	p: BlockVec, // direction
	resid: BlockVec) extends Serializable
{
	def iterate(): ConjGradState =
	{
		// transform p with A
		val Ap: BlockVec = A.multiply(p);
		val alpha: Double = BlockVec.normSquared(resid) / (p.dot(Ap));

		// update x,resid
		val x_new: BlockVec = x + p*alpha;
		val resid_new: BlockVec = resid - (Ap)*alpha;

		val beta: Double = BlockVec.normSquared(resid_new) / BlockVec.normSquared(resid);
		val p_new: BlockVec = resid_new + p*beta;

		//return new state
		ConjGradState(A,x_new,p_new,resid_new);
	}

	def residNorm() = BlockVec.norm(resid,2);

	def residNorm(p: Double) = BlockVec.norm(resid,p);

}
