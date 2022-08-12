using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPORK_EXTRACTOR
{
    public class HanaItemMaster
    {
        public string ItemCode { get; set; }
        public string Prefix { get; set; }
        public string ItemName { get; set; }
        public bool Cancelled { get; set; }
        public string Category { get; set; }
        public string SubCategory { get; set; }
        public string SubSubCategory { get; set; }
        public bool AllowSeniorDiscount { get; set; }
        public bool AllowDecimal { get; set; }
    }

    public class HanaItemUom
    {
        public int UgpEntry { get; set; }
        public string ItemCode { get; set; }
        public string UomCode { get; set; }
        public string Barcode { get; set; }
        public decimal Conversion { get; set; }
        public bool IsBaseUOM { get; set; }
    }

    public class SporkItemMaster : HanaItemMaster
    {

    }

    public class SporkItemUom : HanaItemUom
    {

    }

}
