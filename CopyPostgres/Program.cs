// See https://aka.ms/new-console-template for more information
using BenchmarkDotNet.Running;
using CopyPostgres;
string connectionString = "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=yourpassword;";

//List<Employee> employees = [];
//for (int i = 0; i < 440000; i++)
//{
//    employees.Add(new Employee
//    {
//        Id = i,
//        Name = "ganesh" + i
//    });
//}

//BenchmarkRunner.Run<Helper>();
Console.WriteLine($"started:{DateTime.Now}");
await new Helper().BatchBooking(connectionString);
Console.WriteLine($"completed:{ DateTime.Now}");
Console.ReadLine();
