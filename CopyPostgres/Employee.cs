using BenchmarkDotNet.Attributes;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Buffers;
using System.Text;
using System.Threading.Tasks.Dataflow;

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
            for (int i = 0; i < 1000000; i++)
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

        public async Task BatchBooking()
        {
            //await using var connection = new NpgsqlConnection(connectionString);
            //await connection.OpenAsync();

            string connectionString = "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=yourpassword;";
            var records = new List<Employee>(); // populate this list with your data


            for (int i = 0; i < 1000000; i++)
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

        [Benchmark]
        public async Task ArrayPoolBulkInsertAsync()
        {
            const int bufferSize = 8192;
            var arrayPool = ArrayPool<char>.Shared;
            List<Employee> employees = [];
            for (int i = 0; i < 1000000; i++)
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
            await using var writer = await connection.BeginTextImportAsync("COPY \"public\".\"ganesh\" (id, name) FROM STDIN (FORMAT CSV)");

            char[] buffer = arrayPool.Rent(bufferSize);
            try
            {
                int bufferIndex = 0;

                foreach (var record in employees)
                {
                    var line = $"{record.Id},{EscapeCsvValue(record.Name)}\n";
                    if (line.Length > buffer.Length - bufferIndex)
                    {
                        // If buffer is not large enough to hold the line, write the buffer content
                        await writer.WriteAsync(buffer, 0, bufferIndex);
                        bufferIndex = 0; // Reset the buffer index
                    }

                    // Copy the line into the buffer
                    line.CopyTo(0, buffer, bufferIndex, line.Length);
                    bufferIndex += line.Length;
                }

                // Write remaining buffer content
                if (bufferIndex > 0)
                {
                    await writer.WriteAsync(buffer, 0, bufferIndex);
                }
            }
            finally
            {
                arrayPool.Return(buffer);
            }
        }


        public async Task ParallelBulkInsert()
        {
            const int bufferSize = 8192;
            var arrayPool = ArrayPool<char>.Shared;
            List<Employee> employees = [];
            for (int i = 0; i < 1000000; i++)
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
            await using var writer = await connection.BeginTextImportAsync("COPY \"public\".\"ganesh\" (id, name) FROM STDIN (FORMAT CSV)");

            char[] buffer = arrayPool.Rent(bufferSize);
            var block = new TransformBlock<Employee, string>(record =>
        $"{record.Id},{record.Name}\n",
        new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Environment.ProcessorCount });

            var writeTask = Task.Run(async () =>
            {
                int bufferIndex = 0;
                await foreach (var line in block.ReceiveAllAsync())
                {
                    if (line.Length > buffer.Length - bufferIndex)
                    {
                        // Write the current buffer to the writer
                        await writer.WriteAsync(buffer, 0, bufferIndex);
                        bufferIndex = 0; // Reset the buffer index
                    }

                    // Copy the line into the buffer
                    line.CopyTo(0, buffer, bufferIndex, line.Length);
                    bufferIndex += line.Length;
                }

                if (bufferIndex > 0)
                {
                    // Write the remaining buffer content
                    await writer.WriteAsync(buffer, 0, bufferIndex);
                }
            });

            foreach (var record in employees)
            {
                await block.SendAsync(record);
            }
            block.Complete();
            await writeTask;

            arrayPool.Return(buffer);
        }

        public async Task ParallelBinaryBulkInsertAsync()
        {
            string connectionString = "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=yourpassword;";
            //var employees = new List<Employee>
            //{
            //    new Employee { Id = 1, Name = "Alice" },
            //    new Employee { Id = 2, Name = "Bob" },
            //   // new Employee { Id = 3, Name = null } // Test with null value
            //};

            //            await using var connection = new NpgsqlConnection(connectionString);
            //            await connection.OpenAsync();

            //            await using var writer = await connection.BeginBinaryImportAsync("COPY \"public\".\"ganesh\" (id, name) FROM STDIN (FORMAT BINARY)");

            //            foreach (var record in employees)
            //            {
            //                await writer.StartRowAsync();
            //                await writer.WriteAsync(record.Id, NpgsqlDbType.Integer);
            //                await writer.WriteAsync(record.Name ?? (object)DBNull.Value, NpgsqlDbType.Text);
            //            }

            //            await writer.CompleteAsync();


            List<Employee> employees = [];
            for (int i = 0; i < 1000000; i++)
            {
                employees.Add(new Employee
                {
                    Id = i,
                    Name = "ganesh" + i
                });
            }
            const int bufferSize = 8192; // Define the buffer size
            var arrayPool = ArrayPool<byte>.Shared;
            var buffer = arrayPool.Rent(bufferSize); // Rent a buffer from the array pool

            try
            {
                await using var connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync();

                // Initialize the binary import operation
                await using var writer = await connection.BeginBinaryImportAsync("COPY \"public\".\"ganesh\" (id, name) FROM STDIN (FORMAT BINARY)");

                foreach (var record in employees)
                {
                    // Serialize the data into the buffer
                    int offset = WriteEmployeeToBuffer(record, buffer, 0);

                    using var memoryStream = new MemoryStream(buffer, 0, offset);
                    using var binaryReader = new BinaryReader(memoryStream, Encoding.UTF8);

                    await writer.StartRowAsync();

                    // Read back the values from the memory stream and write to the binary importer
                    writer.Write(binaryReader.ReadInt32(), NpgsqlDbType.Integer);

                    if (binaryReader.ReadBoolean()) // Check if name exists (true means it exists)
                    {
                        var nameLength = binaryReader.ReadInt32(); // Read length of the name
                        var nameBytes = binaryReader.ReadBytes(nameLength); // Read name bytes
                        writer.Write(Encoding.UTF8.GetString(nameBytes), NpgsqlDbType.Text);
                    }
                    else
                    {
                        await writer.WriteNullAsync(); // Handle null name
                    }
                }

                // Complete the binary import
                await writer.CompleteAsync();
            }
            finally
            {
                arrayPool.Return(buffer); // Return the buffer to the array pool
            }
        }

        private int WriteEmployeeToBuffer(Employee record, byte[] buffer, int offset)
        {
            var idBytes = BitConverter.GetBytes(record.Id);

            // Ensure there's enough space in the buffer
            if (offset + idBytes.Length + sizeof(bool) + sizeof(int) + (record.Name?.Length ?? 0) > buffer.Length)
            {
                throw new InvalidOperationException("Buffer overflow. Increase buffer size or handle more efficiently.");
            }

            // Write ID
            Array.Copy(idBytes, 0, buffer, offset, idBytes.Length);
            offset += idBytes.Length;

            // Write existence of name (boolean)
            buffer[offset] = record.Name != null ? (byte)1 : (byte)0;
            offset += sizeof(bool);

            // Write name length and name if it's not null
            if (record.Name != null)
            {
                var nameBytes = Encoding.UTF8.GetBytes(record.Name);
                BitConverter.GetBytes(nameBytes.Length).CopyTo(buffer, offset);
                offset += sizeof(int);
                Array.Copy(nameBytes, 0, buffer, offset, nameBytes.Length);
                offset += nameBytes.Length;
            }

            return offset;
        }

        private string EscapeCsvValue(string value)
        {
            // Escape CSV values
            if (value.Contains(",") || value.Contains("\"") || value.Contains("\n"))
            {
                return "\"" + value.Replace("\"", "\"\"") + "\"";
            }
            return value;
        }
    }

}
