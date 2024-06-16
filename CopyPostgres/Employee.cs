using BenchmarkDotNet.Attributes;
using Npgsql;
using System.Text;

namespace CopyPostgres
{
    public class Employee
    {
        public int Id { get; set; }

        public string? Name { get; set; }
    }

    [MemoryDiagnoser(true)]
    [HtmlExporter]
    public class Helper
    {
        private const string CreateTableSql = @"
        CREATE TABLE IF NOT EXISTS ""public"".""ganesh"" (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
    ";

        [Benchmark]
        public async Task BulkInsertAsync()
        {
            List<Employee> employees = [];
            for (int i = 0; i < 440000; i++)
            {
                employees.Add(new Employee
                {
                    Id = i,
                    Name = "ganesh" + i
                });
            }
            string connectionString = "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=yourpassword;";
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            //    // Ensure the table exists
            //await using (var cmd = new NpgsqlCommand(CreateTableSql, connection))
            //    {
            //        await cmd.ExecuteNonQueryAsync();
            //    }
            var csv = new StringBuilder();
            foreach (var employee in employees)
            {
                // Escape special characters
                var escapedName = employee.Name.Replace("\"", "\"\"");
                csv.AppendLine($"{employee.Id},\"{escapedName}\"");
            }
            await using var writer = await connection.BeginTextImportAsync("COPY \"public\".\"ganesh\" (id, name) FROM STDIN (FORMAT CSV)");
            await writer.WriteAsync(csv.ToString());
        }

        [Benchmark(Baseline = true)]

        public async Task BatchBooking(string connectionString)
        {
            //await using var connection = new NpgsqlConnection(connectionString);
            //await connection.OpenAsync();
            var records = new List<Employee>(); // populate this list with your data


            for (int i = 0; i < 440000; i++)
            {
                records.Add(new Employee
                {
                    Id = i,
                    Name = "ganeshParvati" + i
                });
            }
            //var sql = new StringBuilder("INSERT INTO public.ganesh (id, name) VALUES ");

            //for (int i = 0; i < records.Count; i++)
            //{
            //    sql.Append($"({records[i].Id},{records[i].Name})");
            //    if (i < records.Count - 1) sql.Append(",");
            //}

            //using (var command = new NpgsqlCommand(sql.ToString(), connection))
            //{
            //    command.ExecuteNonQuery();
            //}


            const int batchSize = 1000; // Adjust based on performance needs
            int totalBatches = (int)Math.Ceiling((double)records.Count / batchSize);

            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            for (int batch = 0; batch < totalBatches; batch++)
            {
                int start = batch * batchSize;
                int end = Math.Min(start + batchSize, records.Count);

                var sql = new StringBuilder("INSERT INTO public.ganesh (id, name) VALUES ");
                var parameters = new List<NpgsqlParameter>();

                for (int i = start; i < end; i++)
                {
                    var parameter1 = new NpgsqlParameter($"@p1_{i}", records[i].Id);
                    var parameter2 = new NpgsqlParameter($"@p2_{i}", records[i].Name);

                    sql.Append($"(@p1_{i}, @p2_{i})");
                    if (i < end - 1) sql.Append(",");

                    parameters.Add(parameter1);
                    parameters.Add(parameter2);
                }

                using var command = new NpgsqlCommand(sql.ToString(), connection);
                command.Parameters.AddRange(parameters.ToArray());

                await command.ExecuteNonQueryAsync();
            }

        }
    }

}
