val dishes = Array(
                   Array("pork", 800, "meat"),
                   Array("beef", 700, "meat"),
                   Array("chicken", 400, "meat"),
                   Array("french fries", 530, "other"),
                   Array("rice", 350, "other"),
                   Array("salmon", 450, "fish"))

val dishesRDD = sc.parallelize(dishes)

val dishesByTypeRDD = dishesRDD.map(dish => (dish(2), dish))
dishesByTypeRDD.collect

/* keys: Obtain the keys from a pair RDD */
dishesByTypeRDD.keys.collect
dishesByTypeRDD.keys.distinct.collect

/* countByKey: Count the number of elements for each key, collecting the results in a local Map */
val countDishesByTypeMap = dishesByTypeRDD.countByKey

/* lookup: return the list of values for a given key */
val meatDishesList = dishesByTypeRDD.lookup("meat")
val fishDishesList = dishesByTypeRDD.lookup("fish")

/* mapValues: Pass each value through the mapping function without changing the keys */
def isVegetarian(dish : Array[Any]) : Array[Any] = {
  if (dish(2) == "meat" || dish(2) == "fish") {
    dish :+ false     // :+ Scala array concatenation operator
  } else {
    dish :+ true
  }
}

val dishesWithVegetarianInfoRDD = dishesByTypeRDD.mapValues(isVegetarian)
dishesWithVegetarianInfoRDD.collect

/* flatMapValues: Pass each value through the mapping function without changing the keys and allowing you to add/remove values */

// add side variants for non-vegetarian  dishes

def addSideVariantsForNonVeggieDish(dish: Array[Any]) : Seq[Array[Any]] = {
  if (dish(3) == false) {
    val clonedDish = dish.clone()
    clonedDish(0) = clonedDish(0) + " with fries"
    clonedDish(1) = clonedDish(1).asInstanceOf[Int] + 530
    List(dish, clonedDish)
  } else {
    List(dish)
  }
}


val enhancedDishesRDD = dishesWithVegetarianInfoRDD.flatMapValues(addSideVariantsForNonVeggieDish)
enhancedDishesRDD.collect

// remove "french fries" from the PairRDD
def removeFrenchFriesFromMenu(dish: Array[Any]) : Seq[Array[Any]] = {
  if (dish(0) == "french fries") {
    List()
  } else {
    List(dish)
  }
}

val enhancedDishesMinusFriesRDD = enhancedDishesRDD.flatMapValues(removeFrenchFriesFromMenu)
enhancedDishesMinusFriesRDD.collect

/* reduceByKey: Merge the values for each key using an associative reduce function */
val caloriesViewByTypeRDD = dishesByTypeRDD.mapValues(dish => dish(1))
caloriesViewByTypeRDD.collect

val summaryCaloriesByTypeRDD = caloriesViewByTypeRDD.reduceByKey((calories1, calories2) => calories1.asInstanceOf[Int] + calories2.asInstanceOf[Int])
summaryCaloriesByTypeRDD.collect

/* foldByKey: Merge the values for each key using an associative function and a neutral "zero value" which may be added to the result an arbitrary number of times and must not change the result */
val summaryCaloriesByTypeRDD = caloriesViewByTypeRDD.foldByKey(0)((calories1, calories2) => calories1.asInstanceOf[Int] + calories2.asInstanceOf[Int])
summaryCaloriesByTypeRDD.collect

// using a not neutral zero value has disastrous effects
caloriesViewByTypeRDD.foldByKey(1000)((calories1, calories2) => calories1.asInstanceOf[Int] + calories2.asInstanceOf[Int]).collect

/* aggregateByKey: Aggregate the values of each key, using a neutral "zero value" which may be added to the result an arbitrary number of times and the given transformation and merging functions */
val summaryCaloriesByTypeRDD = dishesByTypeRDD.aggregateByKey(0)((cal, dish) => cal + dish(1).asInstanceOf[Int], (cal1, cal2) => cal1.asInstanceOf[Int] + cal2.asInstanceOf[Int])
summaryCaloriesByTypeRDD.collect
