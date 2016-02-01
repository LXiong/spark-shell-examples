class Dish(val id:Int, val calories:Int, val foodGroup:String) extends Serializable {
  override def toString : String = {
    "Dish [" + "id=" + id + ", calories=" + calories + ", foodGroup=" + foodGroup + "]"
  }
}

class Description(val id:Int, val text:String) extends Serializable {
  override def toString : String = {
    "Description [" + "id=" + id + ", text=" + text + "]"
  }
}


val dishes = List(
                  new Dish(1, 800, "meat"),
                  new Dish(2, 700, "meat"),
                  new Dish(3, 400, "meat"),
                  new Dish(4, 530, "other"),
                  new Dish(5, 350, "other"),
                  new Dish(6, 450, "fish"),
                  new Dish(8, 350, "other"))

val descriptions = List(
                        new Description(1, "pork"),
                        new Description(2, "beef"),
                        new Description(3, "chicken"),
                        new Description(4, "french fries"),
                        new Description(5, "rice"),
                        new Description(6, "salmon"),
                        new Description(7, "halibut"))

val dishesByIdRDD = sc.parallelize(dishes).map(dish => (dish.id, dish))
val descriptionsByIdRDD = sc.parallelize(descriptions).map(desc => (desc.id, desc))

/*
  join: given RDD1: (K, V), RDD2: (K, W)
    RDD1.join(RDD2): (K, (V, W))

    For the keys that exist in only one of the RDDs, the resulting RDD will have no elements.

  RESULT:
    (1,(Dish [id=1, calories=800, foodGroup=meat],Description [id=1, text=pork])),
    (2,(Dish [id=2, calories=700, foodGroup=meat],Description [id=2, text=beef])),
    (3,(Dish [id=3, calories=400, foodGroup=meat],Description [id=3, text=chicken]))
    (4,(Dish [id=4, calories=530, foodGroup=other],Description [id=4, text=french fries])),
    (5,(Dish [id=5, calories=350, foodGroup=other],Description [id=5, text=rice])),
    (6,(Dish [id=6, calories=450, foodGroup=fish],Description [id=6, text=salmon]))
*/

val joinedRDD = dishesByIdRDD.join(descriptionsByIdRDD)
joinedRDD.collect

/*
  leftOuterJoin: given RDD1: (K, V), RDD2: (K, W)
    RDD1.leftOuterJoin(RDD2): (K, (V, Option(W)))

    All the elements from the left RDD will be present in the resulting RDD.
    For the keys that exist only on the left RDD, the resulting RDD will have (K, (V, None)) as its element
    The keys that exist only on the right RDD will not be present.

    RESULT:
    (1,(Dish [id=1, calories=800, foodGroup=meat],Description [id=1, text=pork])),
    (2,(Dish [id=2, calories=700, foodGroup=meat],Description [id=2, text=beef])),
    (3,(Dish [id=3, calories=400, foodGroup=meat],Description [id=3, text=chicken]))
    (4,(Dish [id=4, calories=530, foodGroup=other],Description [id=4, text=french fries])),
    (5,(Dish [id=5, calories=350, foodGroup=other],Description [id=5, text=rice])),
    (6,(Dish [id=6, calories=450, foodGroup=fish],Description [id=6, text=salmon])),
    (8,(Dish [id=8, calories=350, foodGroup=other], None]))
*/

val leftOuterJoinedRDD = dishesByIdRDD.leftOuterJoin(descriptionsByIdRDD)
leftOuterJoinedRDD.collect

/*
  rightOuterJoin: given RDD1: (K, V), RDD2: (K, W)
    RDD1.rightOuterJoin(RDD2): (K, (Option(V), W))

    All the elements from the right RDD will be present in the resulting RDD.
    For the keys that exist only on the right RDD, the resulting RDD will have (K, (None, W)) as its element
    The keys that exist only on the left RDD will not be present.

    RESULT:
    (1,(Dish [id=1, calories=800, foodGroup=meat],Description [id=1, text=pork])),
    (2,(Dish [id=2, calories=700, foodGroup=meat],Description [id=2, text=beef])),
    (3,(Dish [id=3, calories=400, foodGroup=meat],Description [id=3, text=chicken]))
    (4,(Dish [id=4, calories=530, foodGroup=other],Description [id=4, text=french fries])),
    (5,(Dish [id=5, calories=350, foodGroup=other],Description [id=5, text=rice])),
    (6,(Dish [id=6, calories=450, foodGroup=fish],Description [id=6, text=salmon])),
    (7, (None,Description [id=7, text=halibut]))

*/

val rightOuterJoinedRDD = dishesByIdRDD.rightOuterJoin(descriptionsByIdRDD)
rightOuterJoinedRDD.collect

/*
  fullOuterJoin: given RDD1: (K, V), RDD2: (K, W)
    RDD1.fullOuterJoin(RDD2): (K, (Option(V), Option(W)))

    All the elements from the both RDDs will be present in the resulting RDD.
    For the keys that exist only on the left RDD, the resulting RDD will have (K, (V, None)) as its element
    For the keys that exist only on the right RDD, the resulting RDD will have (K, (None, W)) as its element

    RESULT:
    (1,(Dish [id=1, calories=800, foodGroup=meat],Description [id=1, text=pork])),
    (2,(Dish [id=2, calories=700, foodGroup=meat],Description [id=2, text=beef])),
    (3,(Dish [id=3, calories=400, foodGroup=meat],Description [id=3, text=chicken]))
    (4,(Dish [id=4, calories=530, foodGroup=other],Description [id=4, text=french fries])),
    (5,(Dish [id=5, calories=350, foodGroup=other],Description [id=5, text=rice])),
    (6,(Dish [id=6, calories=450, foodGroup=fish],Description [id=6, text=salmon])),
    (7, (None,Description [id=7, text=halibut])),
    (8,(Dish [id=8, calories=350, foodGroup=other], None]))
*/

val fullOuterJoinedRDD = dishesByIdRDD.fullOuterJoin(descriptionsByIdRDD)
fullOuterJoinedRDD.collect


/* Additional convenience methods: subtractByKey */
val missingProductDescriptionsRDD = dishesByIdRDD.subtractByKey(descriptionsByIdRDD)
missingProductDescriptionsRDD.collect
