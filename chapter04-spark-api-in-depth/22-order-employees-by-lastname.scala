case class Employee(firstName:String, lastName: String) extends Ordered[Employee] {
  override def compare(that: Employee) = this.lastName.compare(that.lastName)
}

implicit val emplFirstNameOrdering = new Ordering[Employee] {
  override def compare(a: Employee, b: Employee) = a.firstName.compare(b.firstName)
}

// or simply:
implicit val emplFirstNameOrderingAlt: Ordering[Employee] = Ordering.by(_.firstName)

val employeesAndDepartments = sc.parallelize(List((new Employee("Sergio", "Gonzalez"), "Architecture"),
                                                    (new Employee("Sergio", "Fernandez"), "Frontend"),
                                                    (new Employee("Eloy", "Xomingo"), "Security")))

/*
  Sorted by lastName
*/

val employeesAndDepartmentsSortedByLastName = employeesAndDepartments.sortByKey()
employeesAndDepartmentsSortedByLastName.collect

/*
  Sorted by firstName
*/
val employeesAndDepartmentsSortedByFirstName = employeesAndDepartments.sortByKey(emplFirstNameOrdering)
employeesAndDepartmentsSortedByFirstName.collect
