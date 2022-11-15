using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Infrastracture;

namespace SPORK_BRANCH_WH_EXTRACTOR
{
    public class Controller
    {
        private string hanaServer = Properties.Settings.Default.HanaServer;
        private string hanaUser = Properties.Settings.Default.HanaUser;
        private string hanaPassword = Properties.Settings.Default.HanaPassword;
        private string sporkServer = Properties.Settings.Default.MySQLServer;
        private string sporkDB = Properties.Settings.Default.MySQLDB;
        private string sporkUser = Properties.Settings.Default.MySQLUser;
        private string sporkPass = Properties.Settings.Default.MySQLPassword;
        private string recordLimit = Properties.Settings.Default.RecordCountLimit;

        private string GetConnectionString(Server server)
        {
            string connString = string.Empty;

            if (server == Server.SAPHana)
                connString = $"Server={hanaServer};Username={hanaUser};Password={hanaPassword};";
            else
                connString = $"Server={sporkServer};Database={sporkDB};Username={sporkUser};Password={sporkPass};";

            return connString;
        }

        public List<string> GetExistingWarehouses()
        {
            try
            {
                var list = new List<string>();

                using (var conn = new MySQLHelper(GetConnectionString(Server.MySQL)))
                {
                    conn.ArgMySQLCommand = Query.GetData(DataSource.BranchWarehouse);

                    using (var dr = conn.GetMySQLReader())
                    {
                        while (dr.Read())
                        {
                            list.Add(dr["branchcode"].ToString());
                        }
                    }
                }

                return list;
            }
            catch (Exception)
            {

                throw;
            }
        }

        public List<BranchWarehouse> GetWarehouse(List<string> warehouseCode = null)
        {
            try
            {
                var warehouse = new List<BranchWarehouse>();
                StringBuilder sb = new StringBuilder(),
                    warehouseCodes = new StringBuilder();
                Dictionary<string, object> mysqlParam = new Dictionary<string, object>();


                if (warehouseCode != null)
                {
                    var param = $"'{string.Join("','", warehouseCode)}'";

                    sb = Query.GetData(DataSource.BranchWarehouseNotRegistered, new StringBuilder(param));
                }
                else
                    sb = Query.GetData(DataSource.BranchWarehouseSap);


                using (var conn = new HanaSQLHelper(GetConnectionString(Server.SAPHana)))
                {
                    conn.ArgMySQLCommand = sb;

                    using (var dr = conn.GetMySQLReader())
                    {
                        while (dr.Read())
                        {
                            warehouse.Add(new BranchWarehouse
                            {
                                BranchCode = dr["Code"].ToString(),
                                WarehouseCode = dr["WhsCode"].ToString(),
                                Description = dr["Name"].ToString()
                            });
                        }
                    }
                }

                return warehouse;
            }
            catch
            {

                throw;
            }
        }

        public int Save(List<BranchWarehouse> warehouse)
        {
            try
            {
                Dictionary<string, object> param = new Dictionary<string, object>();
                List<string> parameters = new List<string>();
                StringBuilder sb = new StringBuilder();
                int result = 0;

                sb.Append(Query.InsertWarehouse());

                for (int i = 0; i < warehouse.Count(); i++)
                {
                    parameters.Add($"(@branchcode{i}, @warehousecode{i}, @description{i})");
                    param.Add($"@branchcode{i}", warehouse[i].BranchCode);
                    param.Add($"@warehousecode{i}", warehouse[i].WarehouseCode);
                    param.Add($"@description{i}", warehouse[i].Description);
                }

                sb.Append($"{string.Join(",", parameters)};");

                using (var conn = new MySQLHelper(GetConnectionString(Server.MySQL), sb, param))
                {
                    conn.BeginTransaction();
                    result = conn.ExecuteMySQL();
                    conn.CommitTransaction();
                }

                return result;
            }
            catch
            {

                throw;
            }
        }

    }

    public enum Server
    {
        MySQL, SAPHana
    }

    public enum DataSource
    {
        BranchWarehouse,
        BranchWarehouseSap,
        BranchWarehouseNotRegistered
    }
}
