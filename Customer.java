public class Customer {
    public int id;
    public String name;
    public int age;
    public String gender;
    public int countryCode;
    public float salary;

    public Customer(int id, String name, int age, String gender, int countryCode, float salary){
        this.id = id;
        this.name = name;
        this.age = age;
        this.gender = gender;
        this.countryCode = countryCode;
        this.salary = salary;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public String getGender() {
        return gender;
    }

    public int getCountryCode() {
        return countryCode;
    }

    public float getSalary() {
        return salary;
    }

    public String toString(){
        return getId()+","+getName()+","+getAge()+","+getGender()+","+getCountryCode()+","+getSalary()+"\n";
    }
}
