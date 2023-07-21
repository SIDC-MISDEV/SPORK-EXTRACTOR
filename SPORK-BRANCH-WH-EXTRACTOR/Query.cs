using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPORK_BRANCH_WH_EXTRACTOR
{
    public class Query
    {
        public static StringBuilder GetData(DataSource source, StringBuilder values = null)
        {
            StringBuilder sb = new StringBuilder();
            int recordLimit = Properties.Settings.Default.RecordCountLimit;
            string hanaDB = Properties.Settings.Default.HanaDB;
            string limitData = recordLimit > 0 ? $"LIMIT {recordLimit}" : string.Empty;

            switch (source)
            {
                case DataSource.BranchWarehouse:
                    sb.Append(@"SELECT warehousecode FROM obwhs ORDER BY warehousecode ASC;");
                    break;
                case DataSource.BranchWarehouseSap:
                    //sb.Append($@"SELECT * FROM 
                    //                (SELECT ifnull(b.""Code"",c.""Code"") ""Code"", a.""WhsCode"",a.""WhsName"" ""Name"" FROM {hanaDB}.OWHS a 
                    //                LEFT JOIN {hanaDB}.""@BRANCH"" b ON a.""WhsName"" = b.""Name""
                    //                LEFT JOIN {hanaDB}.""@BRANCH"" c ON a.""WhsCode""= c.""U_Whse"") a 
                    //                WHERE a.""Code"" IS NOT NULL
                    //                ORDER BY ""Code"" ASC {limitData};");
                    sb.Append($@"SELECT ""WhsCode"",""WhsName"",""Branch"" FROM {hanaDB}._vw_sporkwarehouse {limitData}");
                    break;
                case DataSource.BranchWarehouseNotRegistered:
                    //sb.Append($@"SELECT * FROM 
                    //                (SELECT ifnull(b.""Code"",c.""Code"") ""Code"", a.""WhsCode"",a.""WhsName"" ""Name"" FROM {hanaDB}.OWHS a 
                    //                LEFT JOIN {hanaDB}.""@BRANCH"" b ON a.""WhsName"" = b.""Name""
                    //                LEFT JOIN {hanaDB}.""@BRANCH"" c ON a.""WhsCode"" = c.""U_Whse"") a 
                    //                WHERE a.""Code"" IS NOT NULL AND a.""Code"" NOT IN ({values}) ORDER BY ""Code"" {limitData};");
                    sb.Append($@"SELECT ""WhsCode"",""WhsName"",""Branch"" FROM {hanaDB}._vw_sporkwarehouse WHERE ""WhsCode"" NOT IN ({values}) {limitData}");
                    break;
                default:
                    break;
            }

            return sb;
        }

        public static StringBuilder InsertWarehouse()
        {
            return new StringBuilder(@"INSERT INTO obwhs (branchcode, warehousecode, description) VALUES ");
        }
    }
}
