using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPORK_EXTRACTOR
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

        private string GetConnectionString(Server server)
        {
            string connString = string.Empty;

            if(server == Server.SAPHana)
                connString = $"Server={hanaServer};Username={hanaUser};Password={hanaPassword};";
            else
                connString = $"Server={sporkServer};Database={sporkDB};Username={sporkUser};Password={sporkPass};";

            return connString;
        }

        public List<HanaItemMaster> GetItemMasterData(List<string> itemCode = null)
        {
            try
            {
                List<HanaItemMaster> result = new List<HanaItemMaster>();
                
                using (var conn = new HanaSQLHelper(GetConnectionString(Server.SAPHana)))
                {
                    if (itemCode.Count > 0)
                    {
                        //conn.ArgMysqlParam = new Dictionary<string, object>()
                        //{
                        //    { "?itemcode", $"'{ string.Join("','", itemCode)}'" }
                        //};

                        conn.ArgMySQLCommand = SQLQuery.QueryStr(DataSource.HanaMasterItemCode, $"'{ string.Join("','", itemCode)}'");
                    }
                    else
                        conn.ArgMySQLCommand = SQLQuery.QueryStr(DataSource.HanaMasterData);

                    using (var dr = conn.GetMySQLReader())
                    {
                        while (dr.Read())
                        {
                            result.Add(new HanaItemMaster
                            {
                                ItemCode = dr["ItemCode"].ToString(),
                                Prefix = dr["Prefix"].ToString(),
                                ItemName = dr["Name"].ToString(),
                                Cancelled = Convert.ToBoolean(dr["discontinued"]),
                                Category = dr["Category"].ToString(),
                                SubCategory = dr["SubCategory"].ToString(),
                                SubSubCategory = dr["SubSubCategory"].ToString(),
                                AllowSeniorDiscount = Convert.ToBoolean(dr["AllowSeniorDiscount"]),
                                AllowDecimal = Convert.ToBoolean(dr["AllowDecimal"])
                            });
                        }

                        return result;
                    }
                }
            }
            catch
            {

                throw;
            }
        }

        public List<HanaItemUom> GetItemUomData(List<string> itemCode = null)
        {
            try
            {
                List<HanaItemUom> result = new List<HanaItemUom>();

                using (var conn = new HanaSQLHelper(GetConnectionString(Server.SAPHana)))
                {
                    if (itemCode.Count > 0)
                    {
                        //conn.ArgMysqlParam = new Dictionary<string, object>()
                        //{
                        //    { "?ItemCode", $"{ string.Join(",", itemCode)}" }
                        //};

                        conn.ArgMySQLCommand = SQLQuery.QueryStr(DataSource.HanaUomItemCode, $"'{ string.Join("','", itemCode)}'");
                    }
                    else
                        conn.ArgMySQLCommand = SQLQuery.QueryStr(DataSource.HanaUom);

                    using (var dr = conn.GetMySQLReader())
                    {
                        while (dr.Read())
                        {
                            result.Add(new HanaItemUom
                            {
                                UgpEntry = Convert.ToInt32(dr["UgpEntry"]),
                                ItemCode = dr["ID_STOCK"].ToString(),
                                UomCode = dr["UNIT"].ToString(),
                                Barcode = dr["BARCODE"].ToString(),
                                Conversion = Convert.ToDecimal(dr["CONVERSION"]),
                                IsBaseUOM = Convert.ToBoolean(dr["BASE_UOM"])
                            });
                        }

                        return result;
                    }
                }
            }
            catch
            {

                throw;
            }
        }

        public List<string> GetSporkItem()
        {
            try
            {
                List<string> result = new List<string>();

                using (var conn = new MySQLHelper(GetConnectionString(Server.MySQL)))
                {

                    conn.ArgMySQLCommand = SQLQuery.QueryStr(DataSource.SporkMasterData);

                    using (var dr = conn.GetMySQLReader())
                    {
                        while (dr.Read())
                        {
                            result.Add(dr["ItemCode"].ToString());

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

        public List<string> GetSporkUom()
        {
            try
            {
                List<string> result = new List<string>();

                using (var conn = new MySQLHelper(GetConnectionString(Server.MySQL)))
                {

                    conn.ArgMySQLCommand = SQLQuery.QueryStr(DataSource.SporkUom);

                    using (var dr = conn.GetMySQLReader())
                    {
                        while (dr.Read())
                        {
                            result.Add(dr["ItemCode"].ToString());

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

        public int InsertMasterData(List<SporkItemMaster> data)
        {
            try
            {
                Dictionary<string, object> forInsert = new Dictionary<string, object>();
                List<string> param = new List<string>();
                StringBuilder sb = new StringBuilder();
                int result = 0;

                for (int i = 0; i < data.Count; i++)
                {
                    param.Add($"(@itemcode{i}, @prefix{i}, @itemname{i}, @cancelled{i}, @category{i}, @subcategory{i}, @subsubcategory{i}, @seniordiscount{i}, @allowdecimal{i})");
                    forInsert.Add($"@itemcode{i}", data[i].ItemCode);
                    forInsert.Add($"@prefix{i}", data[i].Prefix);
                    forInsert.Add($"@itemname{i}", data[i].ItemName);
                    forInsert.Add($"@cancelled{i}", data[i].Cancelled);
                    forInsert.Add($"@category{i}", data[i].Category);
                    forInsert.Add($"@subcategory{i}", data[i].SubCategory);
                    forInsert.Add($"@subsubcategory{i}", data[i].SubSubCategory);
                    forInsert.Add($"@seniordiscount{i}", data[i].AllowSeniorDiscount);
                    forInsert.Add($"@allowdecimal{i}", data[i].AllowDecimal);
                }

                sb.Append($"{SQLQuery.InsertItemMaster} {string.Join(",", param)}");

                using (var conn = new MySQLHelper(GetConnectionString(Server.MySQL), sb, forInsert))
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

        public int InsertUom(List<SporkItemUom> data)
        {
            try
            {
                Dictionary<string, object> forInsert = new Dictionary<string, object>();
                List<string> param = new List<string>();
                StringBuilder sb = new StringBuilder();
                int result = 0;

                for (int i = 0; i < data.Count; i++)
                {
                    param.Add($"(@UgpEntry{i}, @ItemCode{i}, @UomCode{i}, @Barcode{i}, @Conversion{i}, @baseuom{i})");
                    forInsert.Add($"@UgpEntry{i}", data[i].UgpEntry);
                    forInsert.Add($"@ItemCode{i}", data[i].ItemCode);
                    forInsert.Add($"@UomCode{i}", data[i].UomCode);
                    forInsert.Add($"@Barcode{i}", data[i].Barcode);
                    forInsert.Add($"@Conversion{i}", data[i].Conversion);
                    forInsert.Add($"@baseuom{i}", data[i].IsBaseUOM);
                }

                sb.Append($"{SQLQuery.InsertItemUom} {string.Join(",", param)}");

                using (var conn = new MySQLHelper(GetConnectionString(Server.MySQL), sb, forInsert))
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

    public class SQLQuery
    {
        public static StringBuilder QueryStr(DataSource source, string values = null)
        {
            StringBuilder sb = new StringBuilder();
            string hanaDB = Properties.Settings.Default.HanaDB;
            int recordLimit = Properties.Settings.Default.RecordCountLimit;
            string limitData = recordLimit > 0 ? $"LIMIT {recordLimit}" : string.Empty;

            switch (source)
            {
                case DataSource.HanaMasterData:

                    sb.Append($@"SELECT
			            m.""ItemCode"" as itemcode,
			            CASE WHEN UPPER(c.""ItmsGrpNam"") = 'GROCERIES_KDEXEMPT' THEN 'GRX' ELSE LEFT(m.""ItemCode"",3) END as prefix,
			            m.""ItemName"" as name,
			            CASE WHEN m.""Canceled"" = 'Y' THEN 1 ELSE 0 END as discontinued,
                        UPPER(c.""ItmsGrpNam"") as Category,
                        CASE WHEN LENGTH(m.""U_subcat"") BETWEEN '1' AND '4' THEN d.""Name"" ELSE m.""U_subcat"" END as SubCategory,
                        CASE WHEN LENGTH(m.""U_subcat2"") BETWEEN '1' AND '4'THEN e.""Name"" ELSE m.""U_subcat2"" END as SubSubCategory,
                        CASE WHEN m.""U_Senior"" = 'Y' THEN 1 ELSE 0 END as AllowSeniorDiscount,
                        CASE WHEN m.""U_AllowDecimal"" = 'Y' THEN 1 ELSE 0 END as AllowDecimal
                    FROM {hanaDB}.OITM m
                    INNER JOIN {hanaDB}.OITB c ON m.""ItmsGrpCod"" = c.""ItmsGrpCod""
                    LEFT JOIN {hanaDB}.""@SUBCAT""  d on m.""U_subcat"" = d.""Code""
                    LEFT JOIN {hanaDB}.""@SUBSUBCATEGORY""  e on m.""U_subcat2"" = e.""Code""
                    WHERE
                    (m.""InvntItem"" = 'Y' and m.""SellItem"" = 'Y')
                    AND  m.""ItmsGrpCod"" NOT IN ({Properties.Settings.Default.ItemGroupCode}) ORDER BY m.""ItemCode"" ASC {limitData};");

                    break;
                case DataSource.HanaMasterItemCode:

                    sb.Append($@"SELECT
			            m.""ItemCode"" as itemcode,
			            CASE WHEN UPPER(c.""ItmsGrpNam"") = 'GROCERIES_KDEXEMPT' THEN 'GRX' ELSE LEFT(m.""ItemCode"",3) END as prefix,
			            m.""ItemName"" as name,
			            CASE WHEN m.""Canceled"" = 'Y' THEN 1 ELSE 0 END as discontinued,
                        UPPER(c.""ItmsGrpNam"") as Category,
                        CASE WHEN LENGTH(m.""U_subcat"") BETWEEN '1' AND '4' THEN d.""Name"" ELSE m.""U_subcat"" END as SubCategory,
                        CASE WHEN LENGTH(m.""U_subcat2"") BETWEEN '1' AND '4'THEN e.""Name"" ELSE m.""U_subcat2"" END as SubSubCategory,
                        CASE WHEN m.""U_Senior"" = 'Y' THEN 1 ELSE 0 END as AllowSeniorDiscount,
                        CASE WHEN m.""U_AllowDecimal"" = 'Y' THEN 1 ELSE 0 END as AllowDecimal
		            FROM {hanaDB}.OITM m
                    INNER JOIN {hanaDB}.OITB c ON m.""ItmsGrpCod"" = c.""ItmsGrpCod""
                    LEFT JOIN {hanaDB}.""@SUBCAT""  d on m.""U_subcat"" = d.""Code""
                    LEFT JOIN {hanaDB}.""@SUBSUBCATEGORY""  e on m.""U_subcat2"" = e.""Code""
                    WHERE
                    m.""ItemCode"" NOT IN ({values})
                    AND (m.""InvntItem"" = 'Y' and m.""SellItem"" = 'Y')
                    AND  m.""ItmsGrpCod"" NOT IN ({Properties.Settings.Default.ItemGroupCode}) ORDER BY m.""ItemCode"" ASC {limitData};");

                    break;
                case DataSource.HanaUom:

                    //sb.Append($@"select 
	                   // a.""UgpEntry"",

                    //    a.""UgpCode"" as ID_STOCK,
                    //    c.""UomCode"" as UNIT,
                    //    b.""BaseQty"" as CONVERSION,
                    //    d.""BcdCode"" as BARCODE,
                    //    case c.""UomCode"" when e.""UomCode"" then 1 else 0 end as BASE_UOM
                    //from {hanaDB}.OUGP a
                    //inner
                    //join {hanaDB}.UGP1 b on a.""UgpEntry"" = b.""UgpEntry""
                    //inner join {hanaDB}.OUOM c on b.""UomEntry"" = c.""UomEntry""
                    //inner join {hanaDB}.OITM f on f.""ItemCode"" = a.""UgpCode""
                    //left join {hanaDB}.OBCD d on a.""UgpCode"" = d.""ItemCode"" and b.""UomEntry"" = d.""UomEntry""
                    //left join {hanaDB}.OUOM e on e.""UomEntry"" = a.""BaseUom""
                    //where (f.""InvntItem"" = 'Y' and f.""SellItem"" = 'Y')
                    //AND f.""ItmsGrpCod"" not in ({Properties.Settings.Default.ItemGroupCode})
                    //and c.""UomEntry"" not in (31) 
                    //AND b.""IsActive"" = 'Y' ORDER BY f.""ItemCode"" ASC {limitData}; ");

                    sb.Append($@"select 
	                    a.""UgpEntry"",

                        a.""UgpCode"" as ID_STOCK,
                        c.""UomCode"" as UNIT,
                        b.""BaseQty"" as CONVERSION,
                        d.""BcdCode"" as BARCODE,
                        case c.""UomCode"" when e.""UomCode"" then 1 else 0 end as BASE_UOM
                    from {hanaDB}.OUGP a
                    inner
                    join {hanaDB}.UGP1 b on a.""UgpEntry"" = b.""UgpEntry""
                    inner join {hanaDB}.OUOM c on b.""UomEntry"" = c.""UomEntry""
                    inner join {hanaDB}.OITM f on f.""ItemCode"" = a.""UgpCode""
                    left join {hanaDB}.OBCD d on a.""UgpCode"" = d.""ItemCode"" and b.""UomEntry"" = d.""UomEntry""
                    left join {hanaDB}.OUOM e on e.""UomEntry"" = a.""BaseUom""
                    where (f.""InvntItem"" = 'Y' and f.""SellItem"" = 'Y')
                    AND f.""ItmsGrpCod"" not in ({Properties.Settings.Default.ItemGroupCode})
                    AND b.""IsActive"" = 'Y' ORDER BY f.""ItemCode"" ASC {limitData}; ");


                    break;
                case DataSource.HanaUomItemCode:

                    //sb.Append($@"select 
	                   // a.""UgpEntry"",
                    //    a.""UgpCode"" as ID_STOCK,
                    //    c.""UomCode"" as UNIT,
                    //    b.""BaseQty"" as CONVERSION,
                    //    d.""BcdCode"" as BARCODE,
                    //    case c.""UomCode"" when e.""UomCode"" then 1 else 0 end as BASE_UOM
                    //from {hanaDB}.OUGP a
                    //inner
                    //join {hanaDB}.UGP1 b on a.""UgpEntry"" = b.""UgpEntry""
                    //inner join {hanaDB}.OUOM c on b.""UomEntry"" = c.""UomEntry""
                    //inner join {hanaDB}.OITM f on f.""ItemCode"" = a.""UgpCode""
                    //left join {hanaDB}.OBCD d on a.""UgpCode"" = d.""ItemCode"" and b.""UomEntry"" = d.""UomEntry""
                    //left join {hanaDB}.OUOM e on e.""UomEntry"" = a.""BaseUom""
                    //where f.""ItemCode"" NOT IN ({values})
                    //AND (f.""InvntItem"" = 'Y' and f.""SellItem"" = 'Y')
                    //AND f.""ItmsGrpCod"" not in ({Properties.Settings.Default.ItemGroupCode})
                    //and c.""UomEntry"" not in (31) AND b.""IsActive"" = 'Y' ORDER BY f.""ItemCode"" ASC {limitData}; ");

                    sb.Append($@"select 
	                    a.""UgpEntry"",
                        a.""UgpCode"" as ID_STOCK,
                        c.""UomCode"" as UNIT,
                        b.""BaseQty"" as CONVERSION,
                        d.""BcdCode"" as BARCODE,
                        case c.""UomCode"" when e.""UomCode"" then 1 else 0 end as BASE_UOM
                    from {hanaDB}.OUGP a
                    inner
                    join {hanaDB}.UGP1 b on a.""UgpEntry"" = b.""UgpEntry""
                    inner join {hanaDB}.OUOM c on b.""UomEntry"" = c.""UomEntry""
                    inner join {hanaDB}.OITM f on f.""ItemCode"" = a.""UgpCode""
                    left join {hanaDB}.OBCD d on a.""UgpCode"" = d.""ItemCode"" and b.""UomEntry"" = d.""UomEntry""
                    left join {hanaDB}.OUOM e on e.""UomEntry"" = a.""BaseUom""
                    where f.""ItemCode"" NOT IN ({values})
                    AND (f.""InvntItem"" = 'Y' and f.""SellItem"" = 'Y')
                    AND f.""ItmsGrpCod"" not in ({Properties.Settings.Default.ItemGroupCode})
                    AND b.""IsActive"" = 'Y' ORDER BY f.""ItemCode"" ASC {limitData}; ");

                    break;
                case DataSource.SporkMasterData:

                    sb.Append(@"SELECT ItemCode FROM OITM ORDER BY ItemCode ASC;");

                    break;
                case DataSource.SporkUom:

                    sb.Append(@"SELECT DISTINCT ItemCode FROM oitm_ougp_ugp1_ouom_obcd ORDER BY ItemCode ASC;");

                    break;
                default:
                    break;
            }

            return sb;
        }

        public static StringBuilder InsertItemMaster = new StringBuilder(@"INSERT INTO OITM (ItemCode, Prefix, ItemName, Cancelled, Category, SubCategory, SubSubCategory, AllowSeniorDiscount, AllowDecimalQty) VALUES ");
        public static StringBuilder InsertItemUom = new StringBuilder(@"INSERT INTO oitm_ougp_ugp1_ouom_obcd (UgpEntry, ItemCode, UomCode, Barcode, Conversion, IsBaseUOM) VALUES ");

    }

    public enum Server
    {
        MySQL, SAPHana
    }

    public enum MySQLS
    {
        InsertMaster, InsertUom
    }

    public enum DataSource
    {
        HanaMasterData,
        HanaMasterItemCode,
        HanaUom,
        HanaUomItemCode,
        SporkMasterData,
        SporkUom
    }
}
