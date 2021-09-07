using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SPORK_VENDOR_EXTRACTOR
{
    class Program
    {
        static List<Vendor> vendor = new List<Vendor>();
        static List<string> vendorExisting = new List<string>();

        static string logFile = "Applog.txt";

        static void Main(string[] args)
        {
            try
            {
                ProcessData();
            }
            catch(Exception er)
            {
                Console.WriteLine(er.Message);
                WriteLogs(er.Message);
            }

        }

        static void ProcessData()
        {
            List<Vendor> vendorSapHana = new List<Vendor>();

            Console.WriteLine("Start checking data in SPORK db...");
            //Get saved vendors in spork db
            vendorExisting = GetVendorFromLocal();

            //Check if spork db have saved data.
            if (vendorExisting.Count > 0)
            {
                Console.WriteLine("Getting data to SAP Hana DB...");

                //Get vendors in SAP Hana DB
                vendorSapHana = GetVendors(vendorExisting);

                if (vendorSapHana.Count > 0)
                {
                    Console.WriteLine("Saving data to SPORK db...");
                    SaveVendor(vendorSapHana);
                }
            }
            else
            {
                Console.WriteLine("Getting data to SAP Hana DB...");

                //Get vendors in SAP Hana DB
                vendorSapHana = GetVendors();

                if (vendorSapHana.Count > 0)
                {
                    Console.WriteLine("Saving data to SPORK db...");
                    SaveVendor(vendorSapHana);
                }
            }
        }

        static void WriteLogs(string msg)
        {
            File.AppendAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, logFile), $"{DateTime.Now.ToString()}: {msg}" + Environment.NewLine);
        }

        static List<string> GetVendorFromLocal()
        {
            try
            {
                Controller controller = new Controller();

                return controller.GetExistingVendors();
            }
            catch
            {

                throw;
            }
        }

        static void SaveVendor(List<Vendor> data)
        {
            try
            {
                Controller controller = new Controller();
                int count = 0;

                count = controller.Save(data);

                if(count > 0)
                {
                    WriteLogs($"Successfully saved {count} records.");
                }

            }
            catch
            {

                throw;
            }
        }

        static List<Vendor> GetVendors(List<string> data = null)
        {
            try
            {
                Controller controller = new Controller();

                return controller.GetVendors(data);
            }
            catch
            {

                throw;
            }
        }
    }
}
