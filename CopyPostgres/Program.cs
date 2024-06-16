// See https://aka.ms/new-console-template for more information
using CopyPostgres;
Console.WriteLine($"started:{DateTime.Now.ToLocalTime()}");
await new Helper().ParallelBinaryBulkInsertAsync();
Console.WriteLine($"completed:{ DateTime.Now}");
Console.ReadLine();
