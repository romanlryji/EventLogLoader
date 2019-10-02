//using Nest;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Globalization;

namespace EventLogApp
{
    public class OneEventRecord
    {
        public NLog.Logger Log;

        public long RowID { get; set; }
        public DateTime DateTime { get; set; }
        public long ConnectID { get; set; }
        public long Severity { get; set; }
        public string TransactionStatus { get; set; }
        public string Transaction { get; set; }
        public DateTime TransactionStartTime { get; set; }
        public long TransactionMark { get; set; }
        public long UserName { get; set; }
        public long ComputerName { get; set; }
        public long AppName { get; set; }
        public long EventID { get; set; }
        public string EventType { get; set; }
        public string Comment { get; set; }
        public int MetadataID;
        public long SessionDataSplitCode { get; set; }
        public string DataStructure { get; set; }
        public string DataString { get; set; }
        public long DataType { get; set; }
        public long ServerID { get; set; }
        public long MainPortID { get; set; }
        public long SecondPortID { get; set; }
        public long SessionNumber { get; set; }

        private string eventString;

        public OneEventRecord()
        {
            Log = NLog.LogManager.GetLogger("CurrentThread");
        }


        public OneEventRecord(string eventString)
        {
            CultureInfo provider = CultureInfo.InvariantCulture;

            this.eventString = eventString;

            List<string> parsedEvent = ParserServices.ParseEventLogString(eventString);
            DateTime = DateTime.ParseExact(parsedEvent[0], "yyyyMMddHHmmss", provider);
            TransactionStatus = parsedEvent[1];

            string transactionString = parsedEvent[2].ToString().Replace("}", "").Replace("{", "");

            long transactionDate = From16To10(transactionString.Substring(0, transactionString.IndexOf(",")));

            TransactionStartTime = new DateTime().AddYears(2000);

            try
            {
                if (transactionDate != 0)
                {
                    TransactionStartTime = new DateTime().AddSeconds(Convert.ToInt64(transactionDate / 10000));
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, this.GetType().ToString());
            }

            TransactionMark = From16To10(transactionString.Substring(transactionString.IndexOf(",") + 1));

            Transaction = parsedEvent[2];
            UserName = Convert.ToInt32(parsedEvent[3]);
            ComputerName = Convert.ToInt32(parsedEvent[4]);
            AppName = Convert.ToInt32(parsedEvent[5]);
            EventID = Convert.ToInt32(parsedEvent[7]);
            EventType = parsedEvent[8];
            Comment = parsedEvent[9].RemoveQuotes();
            MetadataID = Convert.ToInt32(parsedEvent[10]);
            DataStructure = parsedEvent[11];
            DataString = parsedEvent[12].RemoveQuotes();
            ServerID = Convert.ToInt32(parsedEvent[13]);
            MainPortID = Convert.ToInt32(parsedEvent[14]);
            SecondPortID = Convert.ToInt32(parsedEvent[15]);
            SessionNumber = Convert.ToInt32(parsedEvent[16]);

            if (DataStructure == "{\"U\"}") //'empty reference
            {
                DataStructure = string.Empty;
            }
            else if (DataStructure.StartsWith("{"))
            {
                //'internal representation for different objects.
                List<string> ParsedObject = ParserServices.ParseEventLogString(DataStructure);

                if (ParsedObject.Count == 2)
                {
                    if (ParsedObject[0] == "\"S\"" || ParsedObject[0] == "\"R\"")  //'this is string or reference 
                        DataStructure = ParsedObject[1].RemoveQuotes(); //'string value
                }
            }

            switch (EventType)
            {
                case "I":
                    Severity = 1;// '"Information";
                    break;
                case "W":
                    Severity = 2;// '"Warning"
                    break;
                case "E":
                    Severity = 3;// '"Error"
                    break;
                case "N":
                    Severity = 4;// '"Note"
                    break;
            }
        }

        public OneEventRecord(SQLiteDataReader reader)
        {
            System.Text.Encoding ANSI = System.Text.Encoding.GetEncoding(1252);
            System.Text.Encoding UTF8 = System.Text.Encoding.UTF8;

            RowID = (long)reader["rowID"];
            Severity = (long)reader["severity"];
            ConnectID = (long)reader["connectID"];
            DateTime = new DateTime().AddSeconds(Convert.ToInt64((long)reader["date"] / 10000));
            TransactionStatus = reader["transactionStatus"].ToString();
            TransactionMark = (long)reader["transactionID"];
            TransactionStartTime = new DateTime().AddYears(2000);

            try
            {
                if ((long)reader["transactionDate"] != 0)
                {
                    TransactionStartTime = new DateTime().AddSeconds(Convert.ToInt64((long)reader["transactionDate"] / 10000));
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, this.GetType().ToString());
            }

            UserName = (long)reader["userCode"];
            ComputerName = (long)reader["computerCode"];
            AppName = (long)reader["appCode"];
            EventID = (long)reader["eventCode"];
            Comment = (string)reader["comment"];

            string MDCodes = (string)reader["metadataCodes"];

            if (string.IsNullOrEmpty(MDCodes))
            {
                MetadataID = 0;
            }
            else if (MDCodes.Contains(","))
            {
                string MDCode = MDCodes.Split(',')[0];
                int.TryParse(MDCode, out MetadataID);
            }
            else
            {
                int.TryParse(MDCodes, out MetadataID);
            }

            string s = string.Empty;

            if (!string.IsNullOrEmpty((string)reader["data"]))
            {
                s = UTF8.GetString(ANSI.GetBytes((string)reader["data"]));
            }

            DataStructure = s;

            DataType = (long)reader["dataType"];
            DataString = (string)reader["dataPresentation"];
            ServerID = (long)reader["workServerCode"];
            MainPortID = (long)reader["primaryPortCode"];
            SecondPortID = (long)reader["secondaryPortCode"];
            SessionNumber = (long)reader["session"];
            SessionDataSplitCode = (long)reader["sessionDataSplitCode"];

            Transaction = string.Empty;
            EventType = string.Empty;
        }


        private long From16To10(string str)
        {
            return long.Parse(str, NumberStyles.HexNumber);
        }

    }
}
