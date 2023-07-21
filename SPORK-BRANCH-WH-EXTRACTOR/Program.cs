using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPORK_BRANCH_WH_EXTRACTOR
{
    class Program
    {
        static List<BranchWarehouse> branchWarehouse = new List<BranchWarehouse>();
        static List<string> branchWarehouseExisting = new List<string>();
        static string logFile = "Applog.txt";

        static void Main(string[] args)
        {
            try
            {
                ProcessData();
            }
            catch (Exception er)
            {
                Console.WriteLine(er.Message);
                WriteLogs(er.Message);
            }
        }

        static void WriteLogs(string msg)
        {
            File.AppendAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, logFile), $"{DateTime.Now.ToString()}: {msg}" + Environment.NewLine);
        }

        static void ProcessData()
        {
            try
            {
                List<BranchWarehouse> branchWHC = new List<BranchWarehouse>();

                Console.WriteLine("Start checking data in SPORK db...");
                //Get saved warehouse in spork db
                branchWarehouseExisting = GetWarehouseFromLocal();

                //Check if spork db have saved data.
                if (branchWarehouseExisting.Count > 0)
                {
                    Console.WriteLine("Getting data to SAP Hana DB...");

                    //Get warehouse in SAP Hana DB
                    branchWHC = GetWarehouse(branchWarehouseExisting);

                    if (branchWHC.Count > 0)
                    {
                        Console.WriteLine("Saving data to SPORK db...");
                        SaveWarehouse(branchWHC);
                    }
                }
                else
                {
                    Console.WriteLine("Getting branch and warehouse data to SAP Hana DB...");

                    //Get vendors in SAP Hana DB
                    branchWHC = GetWarehouse();

                    if (branchWHC.Count > 0)
                    {
                        Console.WriteLine("Saving data to SPORK db...");
                        SaveWarehouse(branchWHC);
                    }
                }
            }
            catch
            {

                throw;
            }
        }

        static List<string> GetWarehouseFromLocal()
        {
            try
            {
                Controller controller = new Controller();

                return controller.GetExistingWarehouses();
            }
            catch
            {

                throw;
            }
        }

        static List<BranchWarehouse> GetWarehouse(List<string> data = null)
        {
            try
            {
                Controller controller = new Controller();

                return controller.GetWarehouse(data);
            }
            catch
            {

                throw;
            }
        }

        static void SaveWarehouse(List<BranchWarehouse> data)
        {
            try
            {
                Controller controller = new Controller();
                int count = 0;

                count = controller.Save(data);

                if (count > 0)
                {
                    WriteLogs($"Successfully saved {count} records.");
                }

            }
            catch
            {

                throw;
            }
        }
    }
}
