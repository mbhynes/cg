package himrod.cg

import himrod.block._

case class ConjGradState(@transient A: BlockMat, //SPD matrix
	x: BlockVec, // current x_k
	p: BlockVec, // direction
	resid: BlockVec) extends Serializable
{
	def iterate(): ConjGradState =
	{
		// matrix-vector multiplication A*p
		val Ap: BlockVec = A.multiply(p);

		// update x,resid
		val alpha: Double = BlockVec.normSquared(resid) / (p.dot(Ap));
		val x_new: BlockVec = x + p*alpha;
		val resid_new: BlockVec = resid - (Ap)*alpha;

		// update search direction
		val beta: Double = BlockVec.normSquared(resid_new) / BlockVec.normSquared(resid);
		val p_new: BlockVec = resid_new + p*beta;

		ConjGradState(A,x_new,p_new,resid_new);
	}

	def residNorm() = BlockVec.norm(resid,2);

	def residNorm(p: Double) = BlockVec.norm(resid,p);
}
