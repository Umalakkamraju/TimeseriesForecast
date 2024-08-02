using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;

namespace StockPrediction
{
    public class StockRecord
    {



        public string StockId { get; set; }
        public DateTime Timestamp { get; set; }
        public decimal Price { get; set; }
    }

    public sealed class StockRecordMap : ClassMap<StockRecord>
    {
        public StockRecordMap()
        {
            Map(m => m.StockId).Index(0);
            Map(m => m.Timestamp).Index(1).TypeConverterOption.Format("dd-MM-yyyy");
            Map(m => m.Price).Index(2);
        }
    }

    class Program
    {





        static void Main(string[] args)
        {
            try
            {
                if (args.Length != 1)
                {
                    Console.WriteLine("Usage: StockPrediction <num-files-to-process>");
                    return;
                }




                if (!int.TryParse(args[0], out int numFilesToProcess) || numFilesToProcess < 1 || numFilesToProcess > 2)
                {
                    Console.WriteLine("The number of files to process must be either 1 or 2.");
                    return;
                }

                // I am taking an assumtion that data is present at the 'Data' and located in the current working directory

                string dataDirectory = "Data"; 

                ProcessStockData(dataDirectory, numFilesToProcess);
            }
            catch (Exception ex)
            {


                Console.WriteLine($"An unexpected error occurred: {ex.Message}");
            }
        }

        private static void ProcessStockData(string directory, int numFiles)
        {
            try
            {
                var exchanges = Directory.GetDirectories(directory);

                
                foreach (var exchange in exchanges)
                {
                    string[] files = Directory.GetFiles(exchange, "*.csv");
                    if (files.Length == 0)
                    {
                        Console.WriteLine($"No files found in {exchange}");
                        continue;
                    }
                    var filesToProcess = files.Take(numFiles).ToArray();

                    foreach (var file in filesToProcess)
                    {


                        try
                        {
                            var records = ReadStockData(file);
                            if (records.Count < 10)
                            {
                                Console.WriteLine($"no enough data in file {file}");
                                continue;
                            }

                            var sampledRecords = GetRandomConsecutiveRecords(records);
                            if (sampledRecords.Count < 10)
                            {
                                Console.WriteLine($"Not sufficient data consecutive data in file {file}");
                                continue;
                            }

                            var predictions = PredictNextValues(sampledRecords);
                            WritePredictionsToFile(file, sampledRecords, predictions);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error while processing file {file}: {ex.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error whie processing stock data: {ex.Message}");
            }
        }

        private static List<StockRecord> ReadStockData(string filePath)
        {
            try
            {
                using (var reader = new StreamReader(filePath))
                using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = false,
                    HeaderValidated = null,
                    MissingFieldFound = null
                }))
                {
                    csv.Context.RegisterClassMap<StockRecordMap>();
                    var records = csv.GetRecords<StockRecord>().ToList();
                    records.Sort((x, y) => x.Timestamp.CompareTo(y.Timestamp));
                    return records;
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Error While reading stock data from {filePath}: {ex.Message}");
            }
        }



        private static List<StockRecord> GetRandomConsecutiveRecords(List<StockRecord> records)
        {
            try
            {
                Random rng = new Random();
                int startIndex = rng.Next(0, records.Count - 10);
                return records.Skip(startIndex).Take(10).ToList();
            }
            catch (Exception ex)
            {
                throw new Exception($"Error getting random consecutive records: {ex.Message}");
            }
        }

        private static Dictionary<DateTime, decimal> PredictNextValues(List<StockRecord> records)
        {
            try
            {
                var predictions = new Dictionary<DateTime, decimal>();

                var sortedPrices = records.Select(r => r.Price).OrderByDescending(p => p).ToList();
                decimal secondHighestPrice = sortedPrices.Skip(1).First();

                decimal priceN1 = secondHighestPrice;
                decimal priceN2 = priceN1 - (records.Last().Price - priceN1) / 2;
                decimal priceN3 = priceN2 - (priceN1 - priceN2) / 4;

                predictions.Add(records.Last().Timestamp.AddDays(1), priceN1);
                predictions.Add(records.Last().Timestamp.AddDays(2), priceN2);
                predictions.Add(records.Last().Timestamp.AddDays(3), priceN3);

                return predictions;
            }
            catch (Exception ex)
            {


                throw new Exception($"Error predicting next values: {ex.Message}");
            }
        }

        private static void WritePredictionsToFile(string inputFilePath, List<StockRecord> records, Dictionary<DateTime, decimal> predictions)
        {
            try
            {
                string outputFilePath = Path.ChangeExtension(inputFilePath, ".predictions.csv");
                using (var writer = new StreamWriter(outputFilePath))
                using (var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture)))
                {
                    foreach (var record in records)
                    {
                        csv.WriteField(record.StockId);
                        csv.WriteField(record.Timestamp.ToString("dd-MM-yyyy"));
                        csv.WriteField(record.Price);
                        csv.NextRecord();
                    }
                    foreach (var prediction in predictions)
                    {
                        csv.WriteField(records.First().StockId);
                        csv.WriteField(prediction.Key.ToString("dd-MM-yyyy"));
                        csv.WriteField(prediction.Value);
                        csv.NextRecord();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Error writing predictions to file: {ex.Message}");
            }
        }
    }
}
