import org.apache.spark.mllib.linalg.distributed.{IndexedRowMatrix, IndexedRow, RowMatrix}


val rmInd = new IndexedRowMatrix(rm.rows.zipWithIndex().map(x => IndexedRow(x._2, x._1))) 
