using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPORK_VENDOR_EXTRACTOR
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

        public List<string> GetExistingVendors()
        {
            try
            {
                var result = new List<string>();

                using (var conn = new MySQLHelper(GetConnectionString(Server.MySQL)))
                {
                    conn.ArgMySQLCommand = SQLQuery.GetData(DataSource.Vendor);

                    using (var dr = conn.GetMySQLReader())
                    {
                        while (dr.Read())
                        {
                            result.Add(dr["vendor_code"].ToString());
                        }
                    }

                    return result;
                }
            }
            catch
            {

                throw;
            }
        }

        public List<Vendor> GetLastUpdatedVendor()
        {
            try
            {
                var result = new List<Vendor>();

                using (var conn = new HanaSQLHelper(GetConnectionString(Server.SAPHana)))
                {
                    conn.ArgMySQLCommand = SQLQuery.GetData(DataSource.UpdatedVendor);

                    using (var dr = conn.GetMySQLReader())
                    {
                        while (dr.Read())
                        {
                            
                            result.Add(new Vendor
                            {
                                VendorCode = dr["vendorcode"].ToString(),
                                VendorName = dr["vendorname"].ToString(),
                                Address = dr["address"].ToString(),
                                Country = dr["country"].ToString(),
                                ZipCode = dr["zipcode"].ToString(),
                                Active = dr["active"].ToString() == "N" ? false : true,
                            });
                        }
                    }

                    return result;
                }
            }
            catch
            {

                throw;
            }
        }

        public List<Vendor> GetVendors(List<string> vendorCode = null)
        {
            try
            {
                var vendors = new List<Vendor>();
                StringBuilder sb = new StringBuilder(),
                    vendorCodes = new StringBuilder();
                Dictionary<string, object> mysqlParam = new Dictionary<string, object>();


                if (vendorCode != null)
                {
                    var param = $"'{string.Join("','", vendorCode)}'";

                    sb = SQLQuery.GetData(DataSource.VendorNotRegistered, new StringBuilder(param));
                }
                else
                    sb = SQLQuery.GetData(DataSource.VendorSap);


                using (var conn = new HanaSQLHelper(GetConnectionString(Server.SAPHana)))
                {
                    conn.ArgMySQLCommand = sb;

                    using (var dr = conn.GetMySQLReader())
                    {
                        while (dr.Read())
                        {
                            vendors.Add(new Vendor
                            {
                                VendorCode = dr["vendorcode"].ToString(),
                                VendorName = dr["vendorname"].ToString(),
                                Address = dr["address"].ToString(),
                                Country = dr["country"].ToString(),
                                ZipCode = dr["zipcode"].ToString(),
                                Active = dr["active"].ToString() == "N" ? false : true,
                            });
                        }
                    }
                }

                return vendors;
            }
            catch
            {

                throw;
            }
        }

        public int Save(List<Vendor> vendors)
        {
            try
            {
                Dictionary<string, object> param = new Dictionary<string, object>();
                List<string> parameters = new List<string>();
                StringBuilder sb = new StringBuilder();
                int result = 0;

                sb.Append(SQLQuery.InsertVendor());

                for (int i = 0; i < vendors.Count(); i++)
                {
                    parameters.Add($"(@vendorcode{i}, @vendorname{i}, @address{i}, @country{i}, @zipcode{i}, @active{i})");
                    param.Add($"@vendorcode{i}", vendors[i].VendorCode);
                    param.Add($"@vendorname{i}", vendors[i].VendorName);
                    param.Add($"@address{i}", vendors[i].Address);
                    param.Add($"@zipcode{i}", vendors[i].ZipCode);
                    param.Add($"@country{i}", vendors[i].Country);
                    param.Add($"@active{i}", vendors[i].Active);
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

        public int Update(List<Vendor> vendors)
        {
            try
            {
                #region OLD
                //Dictionary<string, object> param = new Dictionary<string, object>();
                //List<string> parameters = new List<string>();
                //StringBuilder sb = new StringBuilder();
                //int result = 0;

                //sb.Append(SQLQuery.UpdateVendor());

                //for (int i = 0; i < vendors.Count(); i++)
                //{
                //    //parameters.Add($"(@vendorcode{i}, @vendorname{i}, @address{i}, @country{i}, @zipcode{i}, @active{i})");
                //    param.Add($"@vendorcode{i}", vendors[i].VendorCode);
                //    param.Add($"@vendorname{i}", vendors[i].VendorName);
                //    param.Add($"@address{i}", vendors[i].Address);
                //    param.Add($"@zipcode{i}", vendors[i].ZipCode);
                //    param.Add($"@country{i}", vendors[i].Country);
                //    param.Add($"@active{i}", vendors[i].Active);
                //}

                //sb.Append($"{string.Join(",", parameters)};");

                //using (var conn = new MySQLHelper(GetConnectionString(Server.MySQL), sb, param))
                //{
                //    conn.BeginTransaction();
                //    result = conn.ExecuteMySQL();
                //    conn.CommitTransaction();
                //}
                #endregion
                var result=0;

                using (var conn = new MySQLHelper(GetConnectionString(Server.MySQL)))
                {
                    conn.BeginTransaction();
                    foreach (var item in vendors)
                    {
                        var param = new Dictionary<string, object>()
                        {
                            { "@vendorcode", item.VendorCode },
                            { "@vendorname", item.VendorName },
                            { "@address", item.Address },
                            { "@country", item.Country },
                            { "@zipcode", item.ZipCode },
                            { "@active", item.Active }
                        };
                        
                        conn.ArgMySQLCommand = SQLQuery.UpdateVendor(item.VendorCode);
                        conn.ArgMysqlParam = param;
                        result += conn.ExecuteMySQL();
                    }
                    
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


    public class SQLQuery
    {
        public static StringBuilder GetData(DataSource source, StringBuilder values = null)
        {
            StringBuilder sb = new StringBuilder();
            string recordLimit = Properties.Settings.Default.RecordCountLimit;
            string hanaDB = Properties.Settings.Default.HanaDB;

            switch (source)
            {
                case DataSource.Vendor:
                    sb.Append("SELECT vendor_code FROM ocrd ORDER BY vendor_code ASC");
                    break;
                case DataSource.VendorSap:
                    sb.Append($@"SELECT 
	                        RTRIM(""CardCode"") as VendorCode,
                            RTRIM(""CardName"") as VendorName,
                            RTRIM(""Address"") as Address,
                            ""Country"",
                            ""ZipCode"" as ZipCode,
                            ""validFor"" as Active
                        FROM {hanaDB}.OCRD WHERE ""CardType"" = 'S' AND LEFT(""CardCode"", 3) ='VDR' ORDER BY ""CardCode"" ASC LIMIT {recordLimit}");
                    break;
                case DataSource.VendorNotRegistered:
                    sb.Append($@"SELECT 
	                        RTRIM(""CardCode"") as VendorCode,
                            RTRIM(""CardName"") as VendorName,
                            RTRIM(""Address"") as Address,
                            ""Country"",
                            ""ZipCode"" as ZipCode,
                            ""validFor"" as Active
                        FROM {hanaDB}.OCRD WHERE ""CardType"" = 'S' AND ""CardCode"" NOT IN ({values}) ORDER BY ""CardCode"" ASC LIMIT {recordLimit}");
                    break;
                case DataSource.UpdatedVendor:
                    sb.Append($@"SELECT 
	                        RTRIM(""CardCode"") as VendorCode,
                            RTRIM(""CardName"") as VendorName,
                            RTRIM(""Address"") as Address,
                            ""Country"",
                            ""ZipCode"" as ZipCode,
                            ""validFor"" as Active
                        FROM {hanaDB}.OCRD WHERE ""CardType"" = 'S' AND ""UpdateDate"" is not null AND ""UpdateDate"" = TO_DATE(NOW())  ORDER BY ""CardCode"" ASC LIMIT {recordLimit}");
                    break;
                default:
                    break;
            }

            return sb;
        }

        public static StringBuilder InsertVendor()
        {
            return new StringBuilder(@"INSERT INTO ocrd (vendor_code, vendor_name, address, country, zip_code, active) VALUES ");
        }

        public static StringBuilder UpdateVendor(string vendorCode)
        {
            return new StringBuilder(@"UPDATE ocrd set vendor_name=@vendorname, address=@address, country=@country, zip_code=@zipcode, active=@active WHERE vendor_code=@vendorcode");
        }
}

    public enum Server
    {
        MySQL, SAPHana
    }

    public enum DataSource
    {
        Vendor,
        VendorSap,
        VendorNotRegistered,
        UpdatedVendor
    }
}
