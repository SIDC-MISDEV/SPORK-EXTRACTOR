using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SPORK_EXTRACTOR
{
    class Program
    {
        static List<string> itemCodeMaster = new List<string>(),
            itemCodeUom = new List<string>();

        static List<HanaItemMaster> hanaItem = new List<HanaItemMaster>();
        static List<HanaItemUom> hanaUom = new List<HanaItemUom>();
        static string logFile = "Applog.txt";

        static Controller controller = null;

        static void Main(string[] args)
        {
            try
            {
                //var thread = new Thread(ExecuteProcess);
                //thread.IsBackground = true;
                //thread.Start();
                ExecuteProcess();
            }
            catch (Exception er)
            {
                Console.WriteLine(er.Message);
            }
        }

        static void ExecuteProcess()
        {
            try
            {
                GetSporkItemData();
                GetItemToSapHana();
                SaveData();
            }
            catch (Exception er)
            {
                File.AppendAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, logFile), $"{DateTime.Now}:   {er.Message}.{Environment.NewLine}");
            }
        }

        static void GetSporkItemData()
        {
            try
            {
                controller = new Controller();

                itemCodeMaster = controller.GetSporkItem();

                itemCodeUom = controller.GetSporkUom();
            }
            catch
            {

                throw;
            }
        }

        static void GetItemToSapHana()
        {
            try
            {
                controller = new Controller();

                hanaItem = controller.GetItemMasterData(itemCodeMaster);

                hanaUom = controller.GetItemUomData(itemCodeUom);
            }
            catch
            {

                throw;
            }
        }

        static void SaveData()
        {
            try
            {
                List<SporkItemMaster> itemMaster = null;
                List<SporkItemUom> itemUom = null;
                string message = string.Empty;
                int resultH = 0,
                    resultD = 0;

                if (hanaItem.Count > 0)
                {
                    itemMaster = new List<SporkItemMaster>();
                    controller = new Controller();

                    foreach (var item in hanaItem)
                    {
                        itemMaster.Add(new SporkItemMaster
                        {
                            ItemCode = item.ItemCode,
                            Prefix = item.Prefix,
                            ItemName = item.ItemName,
                            Cancelled = item.Cancelled,
                            Category = item.Category,
                            SubCategory = item.SubCategory,
                            SubSubCategory = item.SubSubCategory
                        });
                    }

                    resultH = controller.InsertMasterData(itemMaster);

                    message = $"{DateTime.Now}: Item Master Data saved successfully. Inserted {resultH} row(s).{Environment.NewLine}";
                    Console.WriteLine(message);
                    File.AppendAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, logFile), message);
                }

                if (hanaUom.Count > 0)
                {
                    itemUom = new List<SporkItemUom>();
                    controller = new Controller();

                    foreach (var item in hanaUom)
                    {
                        itemUom.Add(new SporkItemUom
                        {
                            UgpEntry = item.UgpEntry,
                            ItemCode = item.ItemCode,
                            Barcode = item.Barcode,
                            Conversion = item.Conversion,
                            UomCode = item.UomCode,
                            IsBaseUOM = item.IsBaseUOM
                        });
                    }

                    resultD = controller.InsertUom(itemUom);

                    message = $"{DateTime.Now}: Item Uom Data saved successfully. Inserted {resultD} row(s).{Environment.NewLine}";
                    Console.WriteLine(message);
                    File.AppendAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, logFile), message);

                }
            }
            catch
            {

                throw;
            }
        }
    }
}
